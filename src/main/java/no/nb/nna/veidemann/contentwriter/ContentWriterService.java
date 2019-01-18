/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.contentwriter;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.contentwriter.v1.ContentWriterGrpc;
import no.nb.nna.veidemann.api.contentwriter.v1.CrawledContent;
import no.nb.nna.veidemann.api.contentwriter.v1.RecordType;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteReply;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteRequest;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteRequestMeta;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteResponseMeta;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.contentwriter.text.TextExtractor;
import no.nb.nna.veidemann.contentwriter.warc.SingleWarcWriter;
import no.nb.nna.veidemann.contentwriter.warc.WarcCollection;
import no.nb.nna.veidemann.contentwriter.warc.WarcCollection.Instance;
import no.nb.nna.veidemann.contentwriter.warc.WarcCollectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;

/**
 *
 */
public class ContentWriterService extends ContentWriterGrpc.ContentWriterImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ContentWriterService.class);

    static final byte[] CRLF = {CR, LF};

    private final DbAdapter db;

    private final ConfigAdapter config;

    private final WarcCollectionRegistry warcCollectionRegistry;

    private final TextExtractor textExtractor;

    public ContentWriterService(WarcCollectionRegistry warcCollectionRegistry, TextExtractor textExtractor) {
        this.db = DbService.getInstance().getDbAdapter();
        this.config = DbService.getInstance().getConfigAdapter();
        this.warcCollectionRegistry = warcCollectionRegistry;
        this.textExtractor = textExtractor;
    }

    @Override
    public StreamObserver<WriteRequest> write(StreamObserver<WriteReply> responseObserver) {
        return new StreamObserver<WriteRequest>() {
            private final Map<Integer, ContentBuffer> contentBuffers = new HashMap<>();

            private WriteRequestMeta writeRequestMeta;
            private ConfigObject collectionConfig;
            private boolean canceled = false;

            private ContentBuffer getContentBuffer(Integer recordNum) {
                return contentBuffers.computeIfAbsent(recordNum, n -> new ContentBuffer());
            }

            private WriteRequestMeta.RecordMeta getRecordMeta(int recordNum) throws StatusException {
                WriteRequestMeta.RecordMeta m = writeRequestMeta.getRecordMetaOrDefault(recordNum, null);
                if (m == null) {
                    throw Status.INVALID_ARGUMENT.withDescription("Missing metadata").asException();
                }
                return m;
            }

            @Override
            public void onNext(WriteRequest value) {
                if (writeRequestMeta != null) {
                    MDC.put("eid", writeRequestMeta.getExecutionId());
                    MDC.put("uri", writeRequestMeta.getTargetUri());
                }

                ContentBuffer contentBuffer;
                switch (value.getValueCase()) {
                    case META:
                        writeRequestMeta = value.getMeta();
                        try {
                            if (!writeRequestMeta.hasCollectionRef()) {
                                String msg = "No collection id in request";
                                LOG.error(msg);
                                Status status = Status.INVALID_ARGUMENT.withDescription(msg);
                                responseObserver.onError(status.asException());
                            } else {
                                collectionConfig = config.getConfigObject(writeRequestMeta.getCollectionRef());
                                if (collectionConfig == null || !collectionConfig.hasMeta() || !collectionConfig.hasCollection()) {
                                    String msg = "Collection with id '" + writeRequestMeta.getCollectionRef() + "' is missing or insufficient: " + collectionConfig;
                                    LOG.error(msg);
                                    Status status = Status.UNKNOWN.withDescription(msg);
                                    responseObserver.onError(status.asException());
                                }
                            }
                        } catch (DbException e) {
                            String msg = "Error getting collection config " + writeRequestMeta.getCollectionRef();
                            LOG.error(msg);
                            Status status = Status.UNKNOWN.withDescription(msg);
                            responseObserver.onError(status.asException());
                        }
                        break;
                    case HEADER:
                        contentBuffer = getContentBuffer(value.getHeader().getRecordNum());
                        if (contentBuffer.hasHeader()) {
                            LOG.error("Header received twice");
                            Status status = Status.INVALID_ARGUMENT.withDescription("Header received twice");
                            responseObserver.onError(status.asException());
                            break;
                        }
                        contentBuffer.setHeader(value.getHeader().getData());
                        break;
                    case PAYLOAD:
                        contentBuffer = getContentBuffer(value.getPayload().getRecordNum());
                        contentBuffer.addPayload(value.getPayload().getData());
                        break;
                    case CANCEL:
                        canceled = true;
                        String cancelReason = value.getCancel();
                        LOG.debug("Request cancelled before WARC record written. Reason {}", cancelReason);
                        for (ContentBuffer cb : contentBuffers.values()) {
                            cb.close();
                        }
                        break;
                    default:
                        break;
                }
            }

            @Override
            public void onError(Throwable t) {
                if (writeRequestMeta != null) {
                    MDC.put("uri", writeRequestMeta.getTargetUri());
                    MDC.put("eid", writeRequestMeta.getExecutionId());
                }
                LOG.error("Error caught: {}", t.getMessage(), t);
                for (ContentBuffer contentBuffer : contentBuffers.values()) {
                    contentBuffer.close();
                }
            }

            @Override
            public void onCompleted() {
                if (canceled) {
                    responseObserver.onNext(WriteReply.getDefaultInstance());
                    responseObserver.onCompleted();
                    return;
                }

                if (writeRequestMeta == null) {
                    LOG.error("Missing metadata object");
                    Status status = Status.INVALID_ARGUMENT.withDescription("Missing metadata object");
                    responseObserver.onError(status.asException());
                    return;
                }

                MDC.put("uri", writeRequestMeta.getTargetUri());
                MDC.put("eid", writeRequestMeta.getExecutionId());

                WriteReply.Builder reply = WriteReply.newBuilder();
                // Validate
                for (Entry<Integer, ContentBuffer> recordEntry : contentBuffers.entrySet()) {
                    try {
                        ContentBuffer contentBuffer = recordEntry.getValue();
                        WriteRequestMeta.RecordMeta recordMeta = getRecordMeta(recordEntry.getKey());

                        if (contentBuffer.getTotalSize() == 0L) {
                            LOG.error("Nothing to store");
                            throw Status.INVALID_ARGUMENT.withDescription("Nothing to store").asException();
                        }

                        if (contentBuffer.getTotalSize() != recordMeta.getSize()) {
                            LOG.error("Size mismatch. Expected {}, but was {}",
                                    recordMeta.getSize(), contentBuffer.getTotalSize());
                            throw Status.INVALID_ARGUMENT.withDescription("Size mismatch").asException();
                        }

                        if (!contentBuffer.getBlockDigest().equals(recordMeta.getBlockDigest())) {
                            LOG.error("Block digest mismatch. Expected {}, but was {}",
                                    recordMeta.getBlockDigest(), contentBuffer.getBlockDigest());
                            throw Status.INVALID_ARGUMENT.withDescription("Block digest mismatch").asException();
                        }

                        if (writeRequestMeta.getIpAddress().isEmpty()) {
                            LOG.error("Missing IP-address");
                            throw Status.INVALID_ARGUMENT.withDescription("Missing IP-address").asException();
                        }
                    } catch (StatusException ex) {
                        responseObserver.onError(ex);
                        return;
                    }
                }

                // Write
                List<String> allRecordIds = contentBuffers.values().stream()
                        .map(cb -> cb.getWarcId())
                        .collect(Collectors.toList());

                WarcCollection collection = warcCollectionRegistry.getWarcCollection(collectionConfig);
                try (Instance warcWriters = collection.getWarcWriters()) {
                    for (Entry<Integer, ContentBuffer> recordEntry : contentBuffers.entrySet()) {
                        ContentBuffer contentBuffer = recordEntry.getValue();
                        try {
                            WriteRequestMeta.RecordMeta recordMeta = getRecordMeta(recordEntry.getKey());

                            recordMeta = recordMeta.toBuilder().setPayloadDigest(contentBuffer.getPayloadDigest()).build();

                            if (recordMeta.getType() == RecordType.RESPONSE) {
                                recordMeta = detectRevisit(contentBuffer, recordMeta, collection);
                            }

                            URI ref = writeRecord(warcWriters.getWarcWriter(
                                    recordMeta.getSubCollection()), contentBuffer, writeRequestMeta, recordMeta, allRecordIds);

                            WriteResponseMeta.RecordMeta.Builder responseMeta = WriteResponseMeta.RecordMeta.newBuilder()
                                    .setRecordNum(recordMeta.getRecordNum())
                                    .setType(recordMeta.getType())
                                    .setWarcId(contentBuffer.getWarcId())
                                    .setStorageRef(ref.toString())
                                    .setBlockDigest(contentBuffer.getBlockDigest())
                                    .setPayloadDigest(contentBuffer.getPayloadDigest())
                                    .setWarcRefersTo(recordMeta.getWarcRefersTo())
                                    .setCollectionFinalName(collection.getCollectionName(recordMeta.getSubCollection()));

                            reply.getMetaBuilder().putRecordMeta(responseMeta.getRecordNum(), responseMeta.build());
                        } catch (StatusException ex) {
                            LOG.error("Failed write: {}", ex.getMessage(), ex);
                            responseObserver.onError(ex);
                        } catch (Exception ex) {
                            LOG.error("Failed write: {}", ex.getMessage(), ex);
                            responseObserver.onError(Status.fromThrowable(ex).asException());
                        } finally {
                            contentBuffer.close();
                        }
                    }
                    responseObserver.onNext(reply.build());
                    responseObserver.onCompleted();
                } catch (Exception ex) {
                    Status status = Status.UNKNOWN.withDescription(ex.toString());
                    LOG.error(ex.getMessage(), ex);
                    responseObserver.onError(status.asException());
                }
            }

        };
    }

    private WriteRequestMeta.RecordMeta detectRevisit(final ContentBuffer contentBuffer,
                                                      final WriteRequestMeta.RecordMeta recordMeta,
                                                      final WarcCollection collection) {
        Optional<CrawledContent> isDuplicate = null;
        try {
            isDuplicate = db
                    .hasCrawledContent(CrawledContent.newBuilder()
                            .setDigest(contentBuffer.getPayloadDigest() + ":" + collection.getCollectionName(recordMeta.getSubCollection()))
                            .setWarcId(contentBuffer.getWarcId())
                            .build());
        } catch (DbException e) {
            LOG.error("Failed checking for revisit, treating as new object", e);
            return recordMeta;
        }

        if (isDuplicate.isPresent()) {
            WriteRequestMeta.RecordMeta.Builder recordMetaBuilder = recordMeta.toBuilder();
            recordMetaBuilder.setType(RecordType.REVISIT)
                    .setRecordContentType("application/http")
                    .setBlockDigest(contentBuffer.getHeaderDigest())
                    .setPayloadDigest(isDuplicate.get().getDigest())
                    .setSize(contentBuffer.getHeaderSize())
                    .setWarcRefersTo(isDuplicate.get().getWarcId());

            contentBuffer.removePayload();
            LOG.debug("Detected {} as a revisit of {}",
                    MDC.get("uri"), recordMeta.getWarcRefersTo());
            return recordMetaBuilder.build();
        }
        return recordMeta;
    }

    private URI writeRecord(final SingleWarcWriter warcWriter,
                            final ContentBuffer contentBuffer, final WriteRequestMeta request,
                            final WriteRequestMeta.RecordMeta recordMeta, final List<String> allRecordIds)
            throws StatusException {

        long size = 0L;

        URI ref;

        try {
            ref = warcWriter.writeWarcHeader(contentBuffer.getWarcId(), request, recordMeta, allRecordIds);

            if (contentBuffer.hasHeader()) {
                size += warcWriter.addPayload(contentBuffer.getHeader().newInput());
            }

            if (contentBuffer.hasPayload()) {
                // If both headers and payload are present, add separator
                if (contentBuffer.hasHeader()) {
                    size += warcWriter.addPayload(CRLF);
                }

                long payloadSize = warcWriter.addPayload(contentBuffer.getPayload().newInput());
                if (recordMeta.getType() == RecordType.RESPONSE) {
                    try {
                        textExtractor.analyze(contentBuffer.getWarcId(), request.getTargetUri(), recordMeta.getPayloadContentType(),
                                request.getStatusCode(), contentBuffer.getPayload().newInput(), db);
                    } catch (Exception ex) {
                        LOG.error("Failed extracting text");
                    }
                }

                LOG.debug("Payload of size {}b written for {}", payloadSize, request.getTargetUri());
                size += payloadSize;
            }
        } catch (IOException ex) {
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            LOG.error(ex.getMessage(), ex);
            throw status.asException();
        }

        try {
            warcWriter.closeRecord();
        } catch (IOException ex) {
            if (recordMeta.getSize() != size) {
                Status status = Status.OUT_OF_RANGE.withDescription("Size doesn't match metadata. Expected "
                        + recordMeta.getSize() + ", but was " + size);
                LOG.error(status.getDescription());
                throw status.asException();
            } else {
                Status status = Status.UNKNOWN.withDescription(ex.toString());
                LOG.error(ex.getMessage(), ex);
                throw status.asException();
            }
        }

        return ref;
    }

}
