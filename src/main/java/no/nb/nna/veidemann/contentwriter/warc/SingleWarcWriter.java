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
package no.nb.nna.veidemann.contentwriter.warc;

import no.nb.nna.veidemann.api.config.v1.Collection.SubCollection;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteRequestMeta.RecordMeta;
import no.nb.nna.veidemann.commons.util.Sha1Digest;
import no.nb.nna.veidemann.contentwriter.ContentBuffer;
import no.nb.nna.veidemann.contentwriter.Util;
import no.nb.nna.veidemann.contentwriter.WriteSessionContext.RecordData;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.jwat.warc.WarcFileWriter;
import org.jwat.warc.WarcFileWriterConfig;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;
import static org.jwat.warc.WarcConstants.*;

/**
 *
 */
public class SingleWarcWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SingleWarcWriter.class);

    static final byte[] CRLF = {CR, LF};
    static final String WARC_FILE_SCHEME = "warcfile";

    final WarcFileWriter warcFileWriter;
    final VeidemannWarcFileNaming warcFileNaming;
    final ConfigObject config;
    final SubCollection subCollection;

    public SingleWarcWriter(ConfigObject config, SubCollection subCollection, String filePrefix, File targetDir, String hostName) {
        this.config = config;
        this.subCollection = subCollection;
        warcFileNaming = new VeidemannWarcFileNaming(filePrefix, hostName);
        WarcFileWriterConfig writerConfig = new WarcFileWriterConfig(targetDir, config.getCollection().getCompress(),
                config.getCollection().getFileSize(), false);
        warcFileWriter = WarcFileWriter.getWarcWriterInstance(warcFileNaming, writerConfig);
    }

    public URI writeRecord(final RecordData recordData) throws IOException, SizeMismatchException {
        ContentBuffer contentBuffer = recordData.getContentBuffer();
        long size = 0L;
        boolean newFile;

        try {
            newFile = warcFileWriter.nextWriter();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        File currentFile = warcFileWriter.getFile();
        String finalFileName = currentFile.getName().substring(0, currentFile.getName().length() - 5);

        if (newFile) {
            writeFileDescriptionRecords(finalFileName);
        }

        writeWarcHeader(recordData);

        if (contentBuffer.hasHeader()) {
            size += addPayload(contentBuffer.getHeader().newInput());
        }

        if (contentBuffer.hasPayload()) {
            // If both headers and payload are present, add separator
            if (contentBuffer.hasHeader()) {
                size += addPayload(CRLF);
            }
            long payloadSize = addPayload(contentBuffer.getPayload().newInput());

            LOG.debug("Payload of size {}b written for {}", payloadSize, recordData.getTargetUri());
            size += payloadSize;
        }

        try {
            closeRecord();
        } catch (IllegalStateException e) {
            throw new SizeMismatchException(e.getMessage());
        } catch (IOException ex) {
            if (recordData.getRecordMeta().getSize() != size) {
                SizeMismatchException sizeMismatchException = new SizeMismatchException(recordData.getRecordMeta().getSize(), size);
                sizeMismatchException.initCause(ex);
                throw sizeMismatchException;
            } else {
                throw ex;
            }
        }
        try {
            return new URI(WARC_FILE_SCHEME + ":" + finalFileName + ":" + currentFile.length());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        warcFileWriter.close();
    }

    void writeFileDescriptionRecords(String finalFileName) throws IOException {
        WarcWriter writer = warcFileWriter.getWriter();
        WarcRecord record = WarcRecord.createRecord(writer);
        record.header.major = 1;
        record.header.minor = 0;

        record.header.addHeader(FN_WARC_TYPE, RT_WARCINFO);
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        cal.setTimeInMillis(System.currentTimeMillis());
        record.header.addHeader(FN_WARC_DATE, cal.getTime(), null);
        record.header.addHeader(FN_WARC_FILENAME, finalFileName);
        record.header.addHeader(FN_WARC_RECORD_ID, "<" + warcFileWriter.warcinfoRecordId + ">");
        record.header.addHeader(FN_CONTENT_TYPE, "application/warc-fields");

        Map<String, Object> payload = new HashMap<>();
        payload.put("isPartOf", warcFileNaming.getFilePrefix());
        payload.put("collection", config.getMeta().getName());
        if (subCollection != null) {
            payload.put("subCollection", subCollection.getName());
        }
        payload.put("host", warcFileNaming.getHostName());
        payload.put("format", "WARC File Format 1.0");
        payload.put("description", config.getMeta().getDescription());

        Yaml yaml = new Yaml();

        byte[] payloadBytes = yaml.dumpAsMap(payload).getBytes();
        Sha1Digest payloadDigest = new Sha1Digest();
        payloadDigest.update(payloadBytes);

        record.header.addHeader(FN_CONTENT_LENGTH, payloadBytes.length, null);
        record.header.addHeader(FN_WARC_BLOCK_DIGEST, payloadDigest.getPrefixedDigestString());
        writer.writeHeader(record);

        writer.writePayload(payloadBytes);
        writer.closeRecord();
    }

    void writeWarcHeader(final RecordData recordData) throws IOException {
        WarcWriter writer = warcFileWriter.getWriter();
        WarcRecord record = WarcRecord.createRecord(writer);
        record.header.major = 1;
        record.header.minor = 0;

        record.header.addHeader(FN_WARC_TYPE, Util.getRecordTypeString(recordData.getRecordType()));
        record.header.addHeader(FN_WARC_TARGET_URI, recordData.getTargetUri());
        Date warcDate = Date.from(ProtoUtils.tsToOdt(recordData.getFetchTimeStamp()).toInstant());
        record.header.addHeader(FN_WARC_DATE, warcDate, null);
        record.header.addHeader(FN_WARC_RECORD_ID, Util.formatIdentifierAsUrn(recordData.getWarcId()));

        if (recordData.getRevisitRef() != null) {
            record.header.addHeader(FN_WARC_PROFILE, PROFILE_IDENTICAL_PAYLOAD_DIGEST);
            record.header.addHeader(FN_WARC_REFERS_TO, Util.formatIdentifierAsUrn(recordData.getRevisitRef().getWarcId()));
            if (!recordData.getRevisitRef().getTargetUri().isEmpty() && recordData.getRevisitRef().hasDate()) {
                record.header.addHeader(FN_WARC_REFERS_TO_TARGET_URI,
                        recordData.getRevisitRef().getTargetUri());
                record.header.addHeader(FN_WARC_REFERS_TO_DATE,
                        Date.from(ProtoUtils.tsToOdt(recordData.getRevisitRef().getDate()).toInstant()), null);
            }
        }

        record.header.addHeader(FN_WARC_IP_ADDRESS, recordData.getIpAddress());
        record.header.addHeader(FN_WARC_WARCINFO_ID, "<" + warcFileWriter.warcinfoRecordId + ">");

        RecordMeta recordMeta = recordData.getRecordMeta();
        record.header.addHeader(FN_WARC_BLOCK_DIGEST, recordMeta.getBlockDigest());
        if (!recordMeta.getPayloadDigest().isEmpty()) {
            record.header.addHeader(FN_WARC_PAYLOAD_DIGEST, recordMeta.getPayloadDigest());
        }

        record.header.addHeader(FN_CONTENT_LENGTH, recordMeta.getSize(), null);

        if (!recordMeta.getRecordContentType().isEmpty()) {
            record.header.addHeader(FN_CONTENT_TYPE, recordMeta.getRecordContentType());
        }

        for (String otherId : recordData.getWarcConcurrentToIds()) {
            if (!otherId.equals(recordData.getWarcId())) {
                record.header.addHeader(FN_WARC_CONCURRENT_TO, Util.formatIdentifierAsUrn(otherId));
            }
        }

        writer.writeHeader(record);
    }

    long addPayload(byte[] data) throws UncheckedIOException {
        try {
            return warcFileWriter.getWriter().writePayload(data);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    long addPayload(InputStream data) throws UncheckedIOException {
        try {
            return warcFileWriter.getWriter().streamPayload(data);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    void closeRecord() throws IOException {
        warcFileWriter.getWriter().closeRecord();
    }

    public static class SizeMismatchException extends Exception {
        SizeMismatchException(String message) {
            super(message);
        }

        SizeMismatchException(long expectedSize, long actualSize) {
            super("Size doesn't match metadata. Expected " + expectedSize + ", but was " + actualSize);
        }
    }
}
