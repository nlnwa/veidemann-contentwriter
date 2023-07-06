/*
 * Copyright 2019 National Library of Norway.
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
	"errors"
	"fmt"
	"sync"

	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/veidemann-api/go/config/v1"
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"github.com/nlnwa/veidemann-contentwriter/database"
)

type writeSessionContext struct {
	configCache      database.ConfigCache
	meta             *contentwriter.WriteRequestMeta
	collectionConfig *config.ConfigObject
	recordOpts       []gowarc.WarcRecordOption
	records          map[int32]gowarc.WarcRecord
	recordBuilders   map[int32]gowarc.WarcRecordBuilder
	rbMapSync        sync.Mutex
}

func newWriteSessionContext(configCache database.ConfigCache, recordOpts []gowarc.WarcRecordOption) *writeSessionContext {
	return &writeSessionContext{
		configCache:    configCache,
		recordOpts:     recordOpts,
		records:        make(map[int32]gowarc.WarcRecord),
		recordBuilders: make(map[int32]gowarc.WarcRecordBuilder),
	}
}

func (s *writeSessionContext) setWriteRequestMeta(w *contentwriter.WriteRequestMeta) {
	s.meta = w
}

func (s *writeSessionContext) writeProtocolHeader(header *contentwriter.Data) error {
	recordBuilder := s.getRecordBuilder(header.RecordNum)
	if recordBuilder.Size() != 0 {
		return errors.New("protocol header received twice")
	}
	if _, err := recordBuilder.Write(header.GetData()); err != nil {
		return fmt.Errorf("failed to write protocol header to the record builder: %w", err)
	}
	return nil
}

func (s *writeSessionContext) writePayload(payload *contentwriter.Data) error {
	recordBuilder := s.getRecordBuilder(payload.RecordNum)
	if _, err := recordBuilder.Write(payload.GetData()); err != nil {
		return fmt.Errorf("failed to write payload for record number %d to the record builder: %w", payload.RecordNum, err)
	}
	return nil
}

func (s *writeSessionContext) getRecordBuilder(recordNum int32) gowarc.WarcRecordBuilder {
	s.rbMapSync.Lock()
	defer s.rbMapSync.Unlock()

	if recordBuilder, ok := s.recordBuilders[recordNum]; ok {
		return recordBuilder
	}

	rb := gowarc.NewRecordBuilder(0, s.recordOpts...)

	s.recordBuilders[recordNum] = rb
	return rb
}

func (s *writeSessionContext) validateSession() error {
	if s.meta == nil {
		return errors.New("missing metadata object")
	}
	if s.meta.CollectionRef == nil {
		return errors.New("missing collection ref")
	}
	if s.meta.IpAddress == "" {
		return errors.New("missing IP-address")
	}
	if len(s.meta.TargetUri) == 0 {
		return errors.New("missing target URI")
	}
	collectionConfig, err := s.configCache.GetConfigObject(context.TODO(), s.meta.CollectionRef)
	if err != nil {
		return fmt.Errorf("failed to get collection config: %s", s.meta.GetCollectionRef().GetId())
	}
	if collectionConfig == nil || collectionConfig.Meta == nil || collectionConfig.Spec == nil {
		return fmt.Errorf("collection with id '%s' is missing or insufficient: %s", s.meta.GetCollectionRef().Id, collectionConfig)
	}
	s.collectionConfig = collectionConfig
	for k, rb := range s.recordBuilders {
		recordMeta, ok := s.meta.RecordMeta[k]
		if !ok {
			return fmt.Errorf("missing metadata for record num: %d", k)
		}

		rt := ToGowarcRecordType(recordMeta.Type)
		rb.SetRecordType(rt)
		rb.AddWarcHeader(gowarc.WarcTargetURI, s.meta.TargetUri)
		rb.AddWarcHeader(gowarc.WarcIPAddress, s.meta.IpAddress)
		rb.AddWarcHeaderTime(gowarc.WarcDate, s.meta.FetchTimeStamp.AsTime())
		rb.AddWarcHeaderInt64(gowarc.ContentLength, recordMeta.Size)
		rb.AddWarcHeader(gowarc.ContentType, recordMeta.RecordContentType)
		rb.AddWarcHeader(gowarc.WarcBlockDigest, recordMeta.BlockDigest)
		if recordMeta.PayloadDigest != "" {
			rb.AddWarcHeader(gowarc.WarcPayloadDigest, recordMeta.PayloadDigest)
		}
		for _, wct := range recordMeta.GetWarcConcurrentTo() {
			rb.AddWarcHeader(gowarc.WarcConcurrentTo, "<urn:uuid:"+wct+">")
		}

		wr, val, err := rb.Build()
		if err != nil {
			return fmt.Errorf("failed to build record number %d: %w", k, err)
		}
		if !val.Valid() {
			return fmt.Errorf("failed to validate record number %d: %w", k, val)
		}
		s.records[k] = wr
	}
	return nil
}

func (s *writeSessionContext) cancelSession() {
	for _, rb := range s.recordBuilders {
		_ = rb.Close()
	}
}
