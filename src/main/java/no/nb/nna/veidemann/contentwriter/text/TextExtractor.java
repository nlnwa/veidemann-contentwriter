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
package no.nb.nna.veidemann.contentwriter.text;

import com.google.protobuf.ByteString;
import no.nb.nna.veidemann.api.contentwriter.v1.RecordType;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.contentwriter.ContentBuffer;
import no.nb.nna.veidemann.contentwriter.WriteSessionContext;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.impl.io.DefaultHttpResponseParser;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.SessionInputBufferImpl;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class TextExtractor implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(TextExtractor.class);
    static final ExecutionsAdapter db = DbService.getInstance().getExecutionsAdapter();

    public void extractText(WriteSessionContext.RecordData recordData) {
        ContentBuffer contentBuffer = recordData.getContentBuffer();

        if (recordData.getRecordType() == RecordType.RESPONSE && contentBuffer.getHeader() != null) {
            try {
                HttpResponse httpMessage = getHttpResponse(contentBuffer);
                analyze(recordData.getWarcId(), recordData.getTargetUri(),
                        httpMessage.getFirstHeader("content-type").getValue(),
                        httpMessage.getStatusLine().getStatusCode(),
                        contentBuffer.getPayload().newInput(), db);
            } catch (Exception ex) {
                LOG.error("Failed extracting text: {}", ex.getMessage(), ex);
            }
        }
    }

    public void analyze(String warcId, String targetUri, String contentType, int responseCode, InputStream in, ExecutionsAdapter db) throws IOException {
        MDC.put("uri", targetUri);

        if (shouldParse(targetUri, contentType, responseCode)) {
            AutoDetectParser parser = new AutoDetectParser();

            Metadata metadata = new Metadata();
            metadata.add("warc-id", warcId);

            SkipSpaceContentHandler innerHandler = new SkipSpaceContentHandler(metadata);
            ContentHandler handler = new BodyContentHandler(innerHandler);
            try {
                parser.parse(in, handler, metadata);
                if (metadata.get("Language") != null) {
                    metadata.add("Orig-Content-Type", contentType);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("META:");
                    for (String name : metadata.names()) {
                        System.out.println("\t" + name + ": " + metadata.get(name));
                    }
                }
                if (innerHandler.getExtractedText().getCharacterCount() > 50) {
                    db.addExtractedText(innerHandler.getExtractedText());
                }
            } catch (SAXException | TikaException ex) {
                LOG.warn("Failed reading content ({})", ex.toString(), ex);
            } catch (DbException ex) {
                LOG.warn("Could not write extracted text to DB", ex);
            }
        }
    }

    boolean shouldParse(String targetUri, String contentType, int responseCode) {
        boolean shouldParse = true;

        if (contentType != null
                && (contentType.startsWith("image/")
                || contentType.startsWith("video/")
                || contentType.startsWith("application/x-shockwave-flash")
                || contentType.startsWith("application/binary")
                || contentType.startsWith("application/rss")
                || contentType.startsWith("application/javascript")
                || contentType.startsWith("text/javascript")
                || contentType.startsWith("application/x-javascript")
                || contentType.startsWith("text/css"))) {
            shouldParse = false;
        }
        if (responseCode >= 300) {
            shouldParse = false;
        }

        if (targetUri.startsWith("dns:") || targetUri.endsWith("robots.txt")) {
            shouldParse = false;
        }
        return shouldParse;
    }

    HttpResponse getHttpResponse(ContentBuffer contentBuffer) throws IOException, HttpException {
        ByteString headerBuf = contentBuffer.getHeader();
        SessionInputBufferImpl sessionInputBuffer = new SessionInputBufferImpl(new HttpTransportMetricsImpl(), headerBuf.size());
        sessionInputBuffer.bind(new ByteArrayInputStream(headerBuf.toByteArray()));
        DefaultHttpResponseParser responseParser = new DefaultHttpResponseParser(sessionInputBuffer);
        return responseParser.parse();
    }

    @Override
    public void close() {
    }
}
