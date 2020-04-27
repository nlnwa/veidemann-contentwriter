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
import no.nb.nna.veidemann.api.contentwriter.v1.ContentWriterGrpc;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteReply;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteRequest;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteResponseMeta;
import no.nb.nna.veidemann.contentwriter.WriteSessionContext.RecordData;
import no.nb.nna.veidemann.contentwriter.text.TextExtractor;
import no.nb.nna.veidemann.contentwriter.warc.SingleWarcWriter;
import no.nb.nna.veidemann.contentwriter.warc.WarcCollection;
import no.nb.nna.veidemann.contentwriter.warc.WarcCollection.Instance;
import no.nb.nna.veidemann.contentwriter.warc.WarcCollectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 *
 */
public class ContentWriterService extends ContentWriterGrpc.ContentWriterImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ContentWriterService.class);

    private final WarcCollectionRegistry warcCollectionRegistry;

    private final TextExtractor textExtractor;

    public ContentWriterService(WarcCollectionRegistry warcCollectionRegistry, TextExtractor textExtractor) {
        this.warcCollectionRegistry = warcCollectionRegistry;
        this.textExtractor = textExtractor;
    }

    @Override
    public StreamObserver<WriteRequest> write(StreamObserver<WriteReply> responseObserver) {
        return new StreamObserver<>() {
            private final WriteSessionContext context = new WriteSessionContext();

            @Override
            public void onNext(WriteRequest value) {
                try {
                    context.initMDC();

                    ContentBuffer contentBuffer;
                    switch (value.getValueCase()) {
                        case META:
                            try {
                                context.setWriteRequestMeta(value.getMeta());
                            } catch (StatusException e) {
                                responseObserver.onError(e);
                            }
                            break;
                        case PROTOCOL_HEADER:
                            contentBuffer = context.getRecordData(value.getProtocolHeader().getRecordNum()).getContentBuffer();
                            if (contentBuffer.hasHeader()) {
                                LOG.error("Header received twice");
                                Status status = Status.INVALID_ARGUMENT.withDescription("Header received twice");
                                responseObserver.onError(status.asException());
                                break;
                            }
                            contentBuffer.setHeader(value.getProtocolHeader().getData());
                            break;
                        case PAYLOAD:
                            contentBuffer = context.getRecordData(value.getPayload().getRecordNum()).getContentBuffer();
                            contentBuffer.addPayload(value.getPayload().getData());
                            break;
                        case CANCEL:
                            context.cancelSession(value.getCancel());
                            break;
                        default:
                            break;
                    }
                } catch (Exception ex) {
                    Status status = Status.UNKNOWN.withDescription(ex.toString());
                    LOG.error(ex.getMessage(), ex);
                    responseObserver.onError(status.asException());
                }
            }

            @Override
            public void onError(Throwable t) {
                context.initMDC();
                LOG.error("Error caught: {}", t.getMessage(), t);
                context.cancelSession(t.getMessage());
            }

            @Override
            public void onCompleted() {
                context.initMDC();
                if (context.isCanceled()) {
                    responseObserver.onNext(WriteReply.getDefaultInstance());
                    responseObserver.onCompleted();
                    return;
                }

                if (!context.hasWriteRequestMeta()) {
                    LOG.error("Missing metadata object");
                    Status status = Status.INVALID_ARGUMENT.withDescription("Missing metadata object");
                    responseObserver.onError(status.asException());
                    return;
                }

                WriteReply.Builder reply = WriteReply.newBuilder();
                try {
                    context.validateSession();
                } catch (StatusException e) {
                    responseObserver.onError(e);
                    return;
                } catch (Exception ex) {
                    Status status = Status.UNKNOWN.withDescription(ex.toString());
                    LOG.error(ex.getMessage(), ex);
                    responseObserver.onError(status.asException());
                    return;
                }

                WarcCollection collection = warcCollectionRegistry.getWarcCollection(context.getCollectionConfig());
                try (Instance warcWriters = collection.getWarcWriters()) {
                    for (Integer recordNum : context.getRecordNums()) {
                        try (RecordData recordData = context.getRecordData(recordNum)) {
                            context.detectRevisit(recordNum, collection);

                            URI ref = warcWriters.getWarcWriter(recordData.getSubCollectionType()).writeRecord(recordData);

                            if (context.getCollectionConfig().getCrawlConfig().getExtra().getExtractText()) {
                                textExtractor.extractText(recordData);
                            }

                            WriteResponseMeta.RecordMeta.Builder responseMeta = WriteResponseMeta.RecordMeta.newBuilder()
                                    .setRecordNum(recordNum)
                                    .setType(recordData.getRecordType())
                                    .setWarcId(recordData.getWarcId())
                                    .setStorageRef(ref.toString())
                                    .setBlockDigest(recordData.getContentBuffer().getBlockDigest())
                                    .setPayloadDigest(recordData.getContentBuffer().getPayloadDigest())
                                    .setCollectionFinalName(collection.getCollectionName(recordData.getSubCollectionType()));
                            if (recordData.getRevisitRef() != null) {
                                responseMeta.setRevisitReferenceId(recordData.getRevisitRef().getWarcId());
                            }

                            reply.getMetaBuilder().putRecordMeta(responseMeta.getRecordNum(), responseMeta.build());
                        } catch (IOException ex) {
                            Status status = Status.UNKNOWN.withDescription(ex.toString());
                            LOG.error("Failed write: {}", ex.getMessage(), ex);
                            responseObserver.onError(status.asException());
                        } catch (SingleWarcWriter.SizeMismatchException ex) {
                            Status status = Status.OUT_OF_RANGE.withDescription(ex.getMessage());
                            LOG.error(status.getDescription());
                            throw status.asException();
                        } catch (Exception ex) {
                            LOG.error("Failed write: {}", ex.getMessage(), ex);
                            responseObserver.onError(Status.fromThrowable(ex).asException());
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
}
