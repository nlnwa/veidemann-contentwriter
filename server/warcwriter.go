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
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/veidemann-api/go/config/v1"
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"github.com/nlnwa/veidemann-contentwriter/settings"
	"time"
)

type warcWriter struct {
	fileWriter *gowarc.WarcFileWriter
	timer      *time.Timer
}

func newWarcWriter(s settings.Settings, c *config.ConfigObject, recordMeta *contentwriter.WriteRequestMeta_RecordMeta) *warcWriter {
	collectionConfig := c.GetCollection()
	subCollection := ""
	if recordMeta.GetSubCollection() != config.Collection_UNDEFINED {
		subCollection = recordMeta.GetSubCollection().String()
	}

	prefix := createFilePrefix(c.Meta.Name, subCollection, time.Now(), collectionConfig.GetFileRotationPolicy())
	namer := &gowarc.PatternNameGenerator{
		Directory: s.WarcDir(),
		Prefix:    prefix,
	}

	opts := []gowarc.WarcFileWriterOption{
		gowarc.WithCompression(collectionConfig.GetCompress()),
		gowarc.WithMaxFileSize(collectionConfig.GetFileSize()),
		gowarc.WithFileNameGenerator(namer),
		gowarc.WithWarcInfoFunc(warcInfoGenerator),
		gowarc.WithMaxConcurrentWriters(s.WarcWriterPoolSize()),
	}

	ww := &warcWriter{
		fileWriter: gowarc.NewWarcFileWriter(opts...),
	}

	if d, ok := timeToNextRotation(time.Now(), collectionConfig.GetFileRotationPolicy()); ok {
		ww.timer = time.NewTimer(d)
	}

	return ww
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

func createFilePrefix(collectionName, subCollectionName string, ts time.Time, p config.Collection_RotationPolicy) string {
	if subCollectionName != "" {
		collectionName += "_" + subCollectionName
	}
	dedupRotationKey := createFileRotationKey(ts, p)
	if dedupRotationKey == "" {
		return collectionName + "-"
	} else {
		return collectionName + "_" + dedupRotationKey + "-"
	}
}
