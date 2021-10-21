/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server

import (
	"context"
	"github.com/google/uuid"
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/veidemann-api/go/config/v1"
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"github.com/nlnwa/veidemann-contentwriter/database"
	"github.com/nlnwa/veidemann-contentwriter/settings"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strconv"
	"strings"
	"sync"
	"time"
)

// now is a function so that tests can override the clock.
var now = time.Now

const warcFileScheme = "warcfile"

type warcWriter struct {
	settings         settings.Settings
	collectionConfig *config.ConfigObject
	subCollection    config.Collection_SubCollectionType
	filePrefix       string
	fileWriter       *gowarc.WarcFileWriter
	dbAdapter        database.DbAdapter
	timer            *time.Timer
	done             chan interface{}
	lock             sync.Mutex
	revisitProfile   string
}

func newWarcWriter(s settings.Settings, db database.DbAdapter, c *config.ConfigObject, recordMeta *contentwriter.WriteRequestMeta_RecordMeta) *warcWriter {
	collectionConfig := c.GetCollection()
	ww := &warcWriter{
		settings:         s,
		dbAdapter:        db,
		collectionConfig: c,
		subCollection:    recordMeta.GetSubCollection(),
		filePrefix:       createFilePrefix(c.GetMeta().GetName(), recordMeta.GetSubCollection(), now(), c.GetCollection().GetCollectionDedupPolicy()),
	}
	switch s.WarcVersion() {
	case gowarc.V1_1:
		ww.revisitProfile = gowarc.ProfileIdenticalPayloadDigestV1_1
	case gowarc.V1_0:
		ww.revisitProfile = gowarc.ProfileIdenticalPayloadDigestV1_0
	}
	ww.initFileWriter()

	rotationPolicy := collectionConfig.GetFileRotationPolicy()
	dedupPolicy := collectionConfig.GetCollectionDedupPolicy()
	if dedupPolicy != config.Collection_NONE && dedupPolicy < rotationPolicy {
		rotationPolicy = dedupPolicy
	}
	if d, ok := timeToNextRotation(now(), rotationPolicy); ok {
		ww.timer = time.NewTimer(d)
		ww.done = make(chan interface{})
		go func() {
			for {
				if !ww.waitForTimer(rotationPolicy) {
					break
				}
			}
		}()
	}

	return ww
}

func (ww *warcWriter) CollectionName() string {
	return ww.filePrefix[:len(ww.filePrefix)-1]
}

func (ww *warcWriter) Write(meta *contentwriter.WriteRequestMeta, record ...gowarc.WarcRecord) (*contentwriter.WriteReply, error) {
	ww.lock.Lock()
	defer ww.lock.Unlock()
	revisitKeys := make([]string, len(record))
	for i, r := range record {
		r := r
		record[i], revisitKeys[i] = ww.detectRevisit(int32(i), r, meta)
		defer func() { _ = r.Close() }()
	}
	results := ww.fileWriter.Write(record...)

	reply := &contentwriter.WriteReply{
		Meta: &contentwriter.WriteResponseMeta{
			RecordMeta: map[int32]*contentwriter.WriteResponseMeta_RecordMeta{},
		},
	}

	var err error
	for i, res := range results {
		recNum := int32(i)
		rec := record[i]
		revisitKey := revisitKeys[i]

		if res.Err != nil {
			log.Err(res.Err).Msgf("Error writing record: %s", rec)
		}
		// If writing records failed. Set err to the first error
		if err == nil && res.Err != nil {
			err = res.Err
		}

		// Get WarcRecordId from header: '<urn:uuid:xxxxxxxx-xxx-xxx-xxx-xxxxxxxxx>'
		headerWarcRecordId := rec.WarcHeader().Get(gowarc.WarcRecordID)
		// Trim '<' and '>'
		warcRecordId := strings.TrimSuffix(strings.TrimPrefix(headerWarcRecordId, "<"), ">")
		// Parse as 'urn:uuid:xxxxxxxx-xxx-xxx-xxx-xxxxxxxxx'
		warcId, parseErr := uuid.Parse(warcRecordId)
		if parseErr != nil {
			log.Err(parseErr).Str("warcRecordId", warcRecordId).Msgf("failed to parse %s at %s:%d", gowarc.WarcRecordID, res.FileName, res.FileOffset)
		}

		if res.Err == nil && parseErr == nil && revisitKey != "" {
			if t, err := time.Parse(time.RFC3339, rec.WarcHeader().Get(gowarc.WarcDate)); err != nil {
				log.Err(err).Msg("Could not write CrawledContent to DB")
			} else {
				cr := &contentwriter.CrawledContent{
					Digest:    revisitKey,
					WarcId:    warcId.String(),
					TargetUri: meta.GetTargetUri(),
					Date:      timestamppb.New(t),
				}
				log.Debug().Msgf("Write crawled content: %+v", cr)
				if err := ww.dbAdapter.WriteCrawledContent(context.TODO(), cr); err != nil {
					log.Err(err).Msg("Could not write CrawledContent to DB")
				}
			}
		}
		storageRef := warcFileScheme + ":" + res.FileName + ":" + strconv.FormatInt(res.FileOffset, 10)
		collectionFinalName := ww.filePrefix[:len(ww.filePrefix)-1]
		reply.GetMeta().GetRecordMeta()[recNum] = &contentwriter.WriteResponseMeta_RecordMeta{
			RecordNum:           recNum,
			Type:                FromGowarcRecordType(record[i].Type()),
			WarcId:              warcId.String(),
			StorageRef:          storageRef,
			BlockDigest:         rec.WarcHeader().Get(gowarc.WarcBlockDigest),
			PayloadDigest:       rec.WarcHeader().Get(gowarc.WarcPayloadDigest),
			RevisitReferenceId:  rec.WarcHeader().Get(gowarc.WarcRefersTo),
			CollectionFinalName: collectionFinalName,
		}
	}
	return reply, err
}

func (ww *warcWriter) detectRevisit(recordNum int32, record gowarc.WarcRecord, meta *contentwriter.WriteRequestMeta) (gowarc.WarcRecord, string) {
	if record.Type() == gowarc.Response || record.Type() == gowarc.Resource {
		digest := record.WarcHeader().Get(gowarc.WarcPayloadDigest)
		if digest == "" {
			digest = record.WarcHeader().Get(gowarc.WarcBlockDigest)
		}
		revisitKey := digest + ":" + ww.filePrefix[:len(ww.filePrefix)-1]
		duplicate, err := ww.dbAdapter.HasCrawledContent(context.TODO(), revisitKey)
		if err != nil {
			log.Warn().Err(err).Str("revisitKey", revisitKey).Msg("Failed checking for revisit, treating as new object")
			return record, ""
		}
		log.Debug().Msgf("Duplicate warcId: %v", duplicate.GetWarcId())
		if duplicate != nil {
			log.Debug().Msgf("Detected %s as a revisit of %s",
				record.WarcHeader().Get(gowarc.WarcRecordID), duplicate.GetWarcId())
			ref := &gowarc.RevisitRef{
				Profile:        ww.revisitProfile,
				TargetRecordId: "<urn:uuid:" + duplicate.GetWarcId() + ">",
				TargetUri:      duplicate.GetTargetUri(),
				TargetDate:     duplicate.GetDate().AsTime().In(time.UTC).Format(time.RFC3339),
			}
			revisit, err := record.ToRevisitRecord(ref)
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to create revisit record from %s record, treating as new object", record.Type())
				return record, ""
			}

			newRecordMeta := meta.GetRecordMeta()[recordNum]
			newRecordMeta.Type = contentwriter.RecordType_REVISIT
			newRecordMeta.BlockDigest = revisit.Block().BlockDigest()
			if r, ok := revisit.Block().(gowarc.PayloadBlock); ok {
				newRecordMeta.PayloadDigest = r.PayloadDigest()
			}

			size, err := strconv.ParseInt(revisit.WarcHeader().Get(gowarc.ContentLength), 10, 64)
			if err != nil {
				log.Err(err).Msg("Failed checking for revisit, treating as new object")
				return record, ""
			}
			newRecordMeta.Size = size
			meta.GetRecordMeta()[recordNum] = newRecordMeta
			return revisit, ""
		}
		return record, revisitKey
	}
	return record, ""
}

func (ww *warcWriter) initFileWriter() {
	log.Debug().Msgf("Initializing filewriter with dir: '%s' and file prefix: '%s'", ww.settings.WarcDir(), ww.filePrefix)
	c := ww.collectionConfig.GetCollection()
	namer := &gowarc.PatternNameGenerator{
		Directory: ww.settings.WarcDir(),
		Prefix:    ww.filePrefix,
	}

	opts := []gowarc.WarcFileWriterOption{
		gowarc.WithCompression(c.GetCompress()),
		gowarc.WithMaxFileSize(c.GetFileSize()),
		gowarc.WithFileNameGenerator(namer),
		gowarc.WithWarcInfoFunc(ww.warcInfoGenerator),
		gowarc.WithMaxConcurrentWriters(ww.settings.WarcWriterPoolSize()),
		gowarc.WithAddWarcConcurrentToHeader(true),
		gowarc.WithFlush(ww.settings.FlushRecord()),
		gowarc.WithRecordOptions(gowarc.WithVersion(ww.settings.WarcVersion())),
	}

	ww.fileWriter = gowarc.NewWarcFileWriter(opts...)
}

func (ww *warcWriter) waitForTimer(rotationPolicy config.Collection_RotationPolicy) bool {
	select {
	case <-ww.done:
	case <-ww.timer.C:
		c := ww.collectionConfig.GetCollection()
		prefix := createFilePrefix(ww.collectionConfig.GetMeta().GetName(), ww.subCollection, now(), c.GetCollectionDedupPolicy())
		if prefix != ww.filePrefix {
			ww.lock.Lock()
			defer ww.lock.Unlock()
			ww.filePrefix = prefix
			if err := ww.fileWriter.Close(); err != nil {
				log.Err(err).Msg("failed closing file writer")
			}
			ww.fileWriter = nil
			ww.initFileWriter()
		} else {
			if err := ww.fileWriter.Rotate(); err != nil {
				log.Err(err).Msg("failed rotating file")
			}
		}

		if d, ok := timeToNextRotation(now(), rotationPolicy); ok {
			ww.timer.Reset(d)
		}
		return true
	}

	// We still need to check the return value
	// of Stop, because timer could have fired
	// between the receive on done and this line.
	if !ww.timer.Stop() {
		<-ww.timer.C
	}
	return false
}

func (ww *warcWriter) Shutdown() {
	if ww.timer != nil {
		close(ww.done)
	}
	if err := ww.fileWriter.Close(); err != nil {
		log.Err(err).Msg("failed closing file writer")
	}
}

func timeToNextRotation(now time.Time, p config.Collection_RotationPolicy) (time.Duration, bool) {
	var t2 time.Time

	switch p {
	case config.Collection_HOURLY:
		t2 = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
	case config.Collection_DAILY:
		t2 = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	case config.Collection_MONTHLY:
		t2 = time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, now.Location())
	case config.Collection_YEARLY:
		t2 = time.Date(now.Year()+1, 1, 1, 0, 0, 0, 0, now.Location())
	default:
		return 0, false
	}

	d := t2.Sub(now)
	return d, true
}

func createFileRotationKey(now time.Time, p config.Collection_RotationPolicy) string {
	switch p {
	case config.Collection_HOURLY:
		return now.Format("2006010215")
	case config.Collection_DAILY:
		return now.Format("20060102")
	case config.Collection_MONTHLY:
		return now.Format("200601")
	case config.Collection_YEARLY:
		return now.Format("2006")
	default:
		return ""
	}
}

func createFilePrefix(collectionName string, subCollection config.Collection_SubCollectionType, ts time.Time, dedupPolicy config.Collection_RotationPolicy) string {
	if subCollection != config.Collection_UNDEFINED {
		collectionName += "_" + subCollection.String()
	}

	dedupRotationKey := createFileRotationKey(ts, dedupPolicy)
	if dedupRotationKey == "" {
		return collectionName + "-"
	} else {
		return collectionName + "_" + dedupRotationKey + "-"
	}
}
