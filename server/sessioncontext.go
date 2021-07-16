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
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"time"
)

type writeSessionContext struct {
	log              zerolog.Logger
	configCache      database.ConfigCache
	meta             *contentwriter.WriteRequestMeta
	collectionConfig *config.ConfigObject
	records          map[int32]gowarc.WarcRecord
	recordBuilders   map[int32]gowarc.WarcRecordBuilder
	rbMapSync        sync.Mutex
	canceled         bool
}

func newWriteSessionContext(configCache database.ConfigCache) *writeSessionContext {
	return &writeSessionContext{
		configCache:    configCache,
		records:        make(map[int32]gowarc.WarcRecord),
		recordBuilders: make(map[int32]gowarc.WarcRecordBuilder),
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

func (s *writeSessionContext) getRecordBuilder(recordNum int32) (gowarc.WarcRecordBuilder, error) {
	s.rbMapSync.Lock()
	defer s.rbMapSync.Unlock()

	if recordBuilder, ok := s.recordBuilders[recordNum]; ok {
		return recordBuilder, nil
	}

	var rt gowarc.RecordType
	opts := []gowarc.WarcRecordOption{gowarc.WithStrictValidation()}
	recordMeta, ok := s.meta.RecordMeta[recordNum]
	if !ok {
		return nil, fmt.Errorf("Missing metadata for record #%d", recordNum)
	}

	switch recordMeta.Type {
	case contentwriter.RecordType_WARCINFO:
		rt = gowarc.Warcinfo
	case contentwriter.RecordType_RESPONSE:
		rt = gowarc.Response
	case contentwriter.RecordType_RESOURCE:
		rt = gowarc.Resource
	case contentwriter.RecordType_REQUEST:
		rt = gowarc.Request
	case contentwriter.RecordType_METADATA:
		rt = gowarc.Metadata
	case contentwriter.RecordType_REVISIT:
		rt = gowarc.Revisit
	case contentwriter.RecordType_CONVERSION:
		rt = gowarc.Conversion
	case contentwriter.RecordType_CONTINUATION:
		rt = gowarc.Continuation
	}
	rb := gowarc.NewRecordBuilder(rt, opts...)
	s.recordBuilders[recordNum] = rb
	return rb, nil
}

func (s *writeSessionContext) validateSession() error {
	for k, rb := range s.recordBuilders {
		recordMeta, ok := s.meta.RecordMeta[k]
		if !ok {
			return s.handleErr(codes.InvalidArgument, "Missing metadata for record num: %d", k)
		}
		//ContentBuffer contentBuffer = recordEntry.getValue().getContentBuffer();
		//WriteRequestMeta.RecordMeta recordMeta = writeRequestMeta.getRecordMetaOrDefault(recordEntry.getKey(), null);

		rb.AddWarcHeader(gowarc.WarcIPAddress, s.meta.IpAddress)

		rb.AddWarcHeaderTime(gowarc.WarcDate, time.Now())
		rb.AddWarcHeaderInt64(gowarc.ContentLength, recordMeta.Size)
		rb.AddWarcHeader(gowarc.ContentType, recordMeta.RecordContentType)
		rb.AddWarcHeader(gowarc.WarcBlockDigest, recordMeta.BlockDigest)
		if recordMeta.PayloadDigest != "" {
			rb.AddWarcHeader(gowarc.WarcPayloadDigest, recordMeta.PayloadDigest)
		}
		rb.AddWarcHeader(gowarc.WarcPayloadDigest, recordMeta.PayloadDigest)

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

/*
public class WriteSessionContext {
    private static final Logger LOG = LoggerFactory.getLogger(WriteSessionContext.class);
    private static final ConfigAdapter config = DbService.getInstance().getConfigAdapter();
    private static final ExecutionsAdapter dbAdapter = DbService.getInstance().getExecutionsAdapter();

    //    private final Map<Integer, ContentBuffer> contentBuffers = new HashMap<>();
    final Map<Integer, RecordData> recordDataMap = new HashMap<>();

    WriteRequestMeta.Builder writeRequestMeta;
    private boolean canceled = false;

    public RecordData getRecordData(Integer recordNum) {
        return recordDataMap.computeIfAbsent(recordNum, RecordData::new);
    }

    public boolean hasWriteRequestMeta() {
        return writeRequestMeta != null;
    }

    public ConfigObject getCollectionConfig() {
        return collectionConfig;
    }

    public Set<Integer> getRecordNums() {
        return writeRequestMeta.getRecordMetaMap().keySet();
    }

    public void detectRevisit(final Integer recordNum, final WarcCollection collection) {
        RecordData rd = getRecordData(recordNum);
        if (rd.getRecordType() == RecordType.RESPONSE || rd.getRecordType() == RecordType.RESOURCE) {
            Optional<CrawledContent> isDuplicate = Optional.empty();
            try {
                String digest = rd.getContentBuffer().getPayloadDigest();
                if (digest == null || digest.isEmpty()) {
                    digest = rd.getContentBuffer().getBlockDigest();
                }
                CrawledContent cr = CrawledContent.newBuilder()
                        .setDigest(digest + ":" + collection.getCollectionName(rd.getSubCollectionType()))
                        .setWarcId(rd.getWarcId())
                        .setTargetUri(writeRequestMeta.getTargetUri())
                        .setDate(writeRequestMeta.getFetchTimeStamp())
                        .build();
                isDuplicate = dbAdapter
                        .hasCrawledContent(cr);
            } catch (DbException e) {
                LOG.error("Failed checking for revisit, treating as new object", e);
            }

            if (isDuplicate.isPresent()) {
                CrawledContent cc = isDuplicate.get();
                LOG.debug("Detected {} as a revisit of {}",
                        MDC.get("uri"), cc.getWarcId());

                WriteRequestMeta.RecordMeta newRecordMeta = rd.getRecordMeta().toBuilder()
                        .setType(RecordType.REVISIT)
                        .setBlockDigest(rd.getContentBuffer().getHeaderDigest())
                        .setPayloadDigest(rd.getContentBuffer().getPayloadDigest())
                        .setSize(rd.getContentBuffer().getHeaderSize())
                        .build();
                writeRequestMeta.putRecordMeta(recordNum, newRecordMeta);

                rd.getContentBuffer().removePayload();

                rd.revisitRef = cc;
            }
        }

        if (rd.revisitRef == null) {
            WriteRequestMeta.RecordMeta newRecordMeta = rd.getRecordMeta().toBuilder()
                    .setBlockDigest(rd.getContentBuffer().getBlockDigest())
                    .setPayloadDigest(rd.getContentBuffer().getPayloadDigest())
                    .build();
            writeRequestMeta.putRecordMeta(recordNum, newRecordMeta);
        }
    }

    public class RecordData implements AutoCloseable {
        private final Integer recordNum;
        private final ContentBuffer contentBuffer = new ContentBuffer();
        private CrawledContent revisitRef;

        public RecordData(Integer recordNum) {
            this.recordNum = recordNum;
        }

        public ContentBuffer getContentBuffer() {
            return contentBuffer;
        }

        public WriteRequestMeta.RecordMeta getRecordMeta() {
            return writeRequestMeta.getRecordMetaOrThrow(recordNum);
        }

        public CrawledContent getRevisitRef() {
            return revisitRef;
        }

        public String getWarcId() {
            return contentBuffer.getWarcId();
        }

        public RecordType getRecordType() {
            return getRecordMeta().getType();
        }

        public SubCollectionType getSubCollectionType() {
            return getRecordMeta().getSubCollection();
        }

        public String getTargetUri() {
            return writeRequestMeta.getTargetUri();
        }

        public Timestamp getFetchTimeStamp() {
            return writeRequestMeta.getFetchTimeStamp();
        }

        public String getIpAddress() {
            return writeRequestMeta.getIpAddress();
        }

        public List<String> getWarcConcurrentToIds() {
            List<String> ids = recordDataMap.values().stream()
                    .map(cb -> cb.getContentBuffer().getWarcId())
                    .collect(Collectors.toList());
            ids.addAll(getRecordMeta().getWarcConcurrentToList());
            return ids;
        }

        public void close() {
            contentBuffer.close();
        }
    }
}
*/
