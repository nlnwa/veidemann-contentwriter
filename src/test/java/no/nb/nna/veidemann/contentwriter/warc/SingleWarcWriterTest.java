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

import com.google.protobuf.ByteString;
import no.nb.nna.veidemann.api.config.v1.Collection;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.contentwriter.v1.RecordType;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteRequestMeta;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.DbServiceSPI;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.contentwriter.ContentBuffer;
import no.nb.nna.veidemann.contentwriter.WriteSessionContext;
import no.nb.nna.veidemann.contentwriter.WriteSessionContextBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class SingleWarcWriterTest {

    private final static String requestHeader = "Host: elg.no\n" +
            "User-Agent: Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:69.0) Gecko/20100101 Firefox/69.0\n" +
            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\n" +
            "Accept-Language: nb-NO,nb;q=0.9,no-NO;q=0.8,no;q=0.6,nn-NO;q=0.5,nn;q=0.4,en-US;q=0.3,en;q=0.1\n" +
            "Accept-Encoding: gzip, deflate\n" +
            "Connection: keep-alive\n" +
            "Upgrade-Insecure-Requests: 1\n";

    private final static String responseHeader = "HTTP/1.1 200 OK\n" +
            "Date: Thu, 03 Oct 2019 09:17:44 GMT\n" +
            "Content-Type: text/html\n" +
            "Transfer-Encoding: chunked\n" +
            "Connection: keep-alive\n" +
            "Set-Cookie: __cfduid=deafaee9d9fb85ec39631049d132a1e481570094263; expires=Fri, 02-Oct-20 09:17:43 GMT; path=/; domain=.elg.no; HttpOnly\n" +
            "Last-Modified: Wed, 11 Sep 2019 10:56:04 GMT\n" +
            "CF-Cache-Status: DYNAMIC\n" +
            "Server: cloudflare\n" +
            "CF-RAY: 51fdd31d6a85d895-CPH\n" +
            "Content-Encoding: gzip\n";

    private final static String responsePayload = "<!doctype html>\n" +
            "<meta charset=utf-8>\n" +
            "<html>\n" +
            "<head>\n" +
            "<style>\n" +
            "body {\n" +
            "\tmargin-top: 30px;\n" +
            "\tfont-family: Arial, Helvetica;\n" +
            "\ttext-align: center;\n" +
            "}\n" +
            "h1 {\n" +
            "\tcolor:#000000;\n" +
            "\tfont-size: 18px;\n" +
            "\tfont-weight: 700;\n" +
            "}\n" +
            "p {\n" +
            "\tfont-size: 16px;\n" +
            "\tcolor:#000000;\n" +
            "\tfont-weight: 400;\n" +
            "}\n" +
            "p.small {\n" +
            "\tmargin-top: 60px;\n" +
            "\tfont-size: 10px;\n" +
            "\tcolor:#000000;\n" +
            "\tfont-weight: 400;\n" +
            "}\n" +
            "\n" +
            "</style>\n" +
            "</head>\n" +
            "\n" +
            "<body>\n" +
            "<h1>www.elg.no</h1>\n" +
            "<img src=\"elg.jpg\">\n" +
            "<p>Elger er gromme dyr.<br>\n" +
            "Elgkalvene er mat for bl.a. ulv.</p>\n" +
            "<p class=\"small\">Last ned Vivaldi på <a href=\"https://vivaldi.com\">vivaldi.com</a></p>\n" +
            "</body>\n" +
            "</html>\n";

    private static final String hostName = "test-host";

    private static final String filePrefix = "test";


    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * Test of write method, of class SingleWarcWriter.
     */
    @Test
    public void testWrite() throws Exception {
        DbServiceSPI dbProviderMock = mock(DbServiceSPI.class);
        when(dbProviderMock.getExecutionsAdapter()).thenReturn(mock(ExecutionsAdapter.class));
        DbService.configure(dbProviderMock);

        final File targetDir = temporaryFolder.getRoot();
        final boolean compress = false;

        final Collection collection = Collection.newBuilder()
                .setCompress(compress)
                .setFileSize(2048)
                .build();

        final ConfigObject collectionConfig = ConfigObject.newBuilder()
                .setCollection(collection)
                .build();


        final WriteRequestMeta.Builder writeRequestMeta = WriteRequestMeta.newBuilder()
                .setTargetUri("http://elg.no")
                .setIpAddress("104.24.117.137");

        final WriteSessionContext context = new WriteSessionContextBuilder()
                .withWriteRequestMeta(writeRequestMeta)
                .withCollectionConfig(collectionConfig)
                .build();

        // Request
        ByteString header0 = ByteString.copyFromUtf8(requestHeader);

        WriteSessionContext.RecordData recordData0 = context.getRecordData(0);
        ContentBuffer contentBuffer0 = recordData0.getContentBuffer();
        contentBuffer0.setHeader(header0);

        WriteRequestMeta.RecordMeta rm0 = WriteRequestMeta.RecordMeta.newBuilder()
                .setSize(contentBuffer0.getTotalSize())
                .setType(RecordType.REQUEST)
                .build();
        writeRequestMeta.putRecordMeta(0, rm0);

        // Response
        ByteString header1 = ByteString.copyFromUtf8(responseHeader);
        ByteString payload1 = ByteString.copyFromUtf8(responsePayload);

        WriteSessionContext.RecordData recordData1 = context.getRecordData(1);
        ContentBuffer contentBuffer1 = recordData1.getContentBuffer();
        contentBuffer1.setHeader(header1);
        contentBuffer1.addPayload(payload1);

        WriteRequestMeta.RecordMeta rm1 = WriteRequestMeta.RecordMeta.newBuilder()
                .setType(RecordType.RESPONSE)
                .setRecordContentType("text/html")
                .setSize(contentBuffer1.getTotalSize())
                .build();

        writeRequestMeta.putRecordMeta(1, rm1);

        try (SingleWarcWriter writer = new SingleWarcWriter(collectionConfig, null, filePrefix, targetDir, hostName)) {
            for (Integer recordNum : context.getRecordNums()) {
                try (WriteSessionContext.RecordData recordData = context.getRecordData(recordNum)) {
                    writer.writeRecord(recordData);
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                    throw e;
                }
            }
        }


        final File[] files = targetDir.listFiles();
        assertThat(files).isNotNull();
        for (final File f : files) {
            assertThat(f).hasExtension(compress ? "gz" : "warc");
        }
    }
}
