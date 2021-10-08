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
	"fmt"
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/veidemann-api/go/config/v1"
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"github.com/nlnwa/veidemann-contentwriter/database"
	"github.com/nlnwa/veidemann-contentwriter/settings"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

type writeSessionContext struct {
	log              zerolog.Logger
	settings         settings.Settings
	configCache      database.ConfigCache
	meta             *contentwriter.WriteRequestMeta
	collectionConfig *config.ConfigObject
	recordOpts       []gowarc.WarcRecordOption
	records          map[int32]gowarc.WarcRecord
	recordBuilders   map[int32]gowarc.WarcRecordBuilder
	payloadStarted   map[int32]bool
	rbMapSync        sync.Mutex
	canceled         bool
}

func newWriteSessionContext(configCache database.ConfigCache, recordOpts []gowarc.WarcRecordOption) *writeSessionContext {
	return &writeSessionContext{
		configCache:    configCache,
		recordOpts:     recordOpts,
		records:        make(map[int32]gowarc.WarcRecord),
		recordBuilders: make(map[int32]gowarc.WarcRecordBuilder),
		payloadStarted: make(map[int32]bool),
		log:            log.Logger,
	}
}

func (s *writeSessionContext) handleErr(code codes.Code, msg string, args ...interface{}) error {
	m := fmt.Sprintf(msg, args...)
	s.log.Error().Msg(m)
	return status.Error(code, m)
}

func (s *writeSessionContext) setWriteRequestMeta(w *contentwriter.WriteRequestMeta) error {
	if s.meta == nil {
		s.log = log.With().Str("eid", w.ExecutionId).Str("uri", w.TargetUri).Logger()
	}
	s.meta = w

	if w.CollectionRef == nil {
		return s.handleErr(codes.InvalidArgument, "No collection id in request")
	}
	if w.IpAddress == "" {
		return s.handleErr(codes.InvalidArgument, "Missing IP-address")
	}

	collectionConfig, err := s.configCache.GetConfigObject(context.TODO(), w.GetCollectionRef())
	if err != nil {
		msg := "Error getting collection config " + w.GetCollectionRef().GetId()
		s.log.Error().Msg(msg)
		return status.Error(codes.Unknown, msg)
	}
	s.collectionConfig = collectionConfig
	if collectionConfig == nil || collectionConfig.Meta == nil || collectionConfig.Spec == nil {
		return s.handleErr(codes.Unknown, "Collection with id '%s' is missing or insufficient: %s", w.CollectionRef.Id, collectionConfig.String())
	}
	return nil
}

func (s *writeSessionContext) writeProtocolHeader(header *contentwriter.Data) error {
	recordBuilder := s.getRecordBuilder(header.RecordNum)
	if recordBuilder.Size() != 0 {
		err := s.handleErr(codes.InvalidArgument, "Header received twice")
		s.cancelSession(err.Error())
		return err
	}
	if _, err := recordBuilder.Write(header.GetData()); err != nil {
		s.cancelSession(err.Error())
		return err
	}
	return nil
}

func (s *writeSessionContext) writePayoad(payload *contentwriter.Data) error {
	recordBuilder := s.getRecordBuilder(payload.RecordNum)
	if !s.payloadStarted[payload.RecordNum] {
		if _, err := recordBuilder.Write([]byte("\r\n")); err != nil {
			s.cancelSession(err.Error())
			return err
		}
		s.payloadStarted[payload.RecordNum] = true
	}
	if _, err := recordBuilder.Write(payload.GetData()); err != nil {
		s.cancelSession(err.Error())
		return err
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
	for k, rb := range s.recordBuilders {
		recordMeta, ok := s.meta.RecordMeta[k]
		if !ok {
			return s.handleErr(codes.InvalidArgument, "Missing metadata for record num: %d", k)
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
			rb.AddWarcHeader(gowarc.WarcConcurrentTo, "<"+wct+">")
		}

		wr, _, err := rb.Build()
		if err != nil {
			return s.handleErr(codes.InvalidArgument, "Error: %s", err)
		}
		s.records[k] = wr
	}
	return nil
}

func (s *writeSessionContext) cancelSession(cancelReason string) {
	s.canceled = true
	s.log.Debug().Msgf("Request cancelled before WARC record written. Reason %s", cancelReason)
	for _, rb := range s.recordBuilders {
		_ = rb.Close()
	}
}
