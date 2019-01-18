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

package no.nb.nna.veidemann.contentwriter;

import com.google.protobuf.ByteString;
import com.rethinkdb.RethinkDB;
import io.grpc.StatusException;
import no.nb.nna.veidemann.api.config.v1.Collection.SubCollectionType;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.contentwriter.v1.Data;
import no.nb.nna.veidemann.api.contentwriter.v1.RecordType;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteRequestMeta;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteRequestMeta.RecordMeta;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.client.ContentWriterClient.ContentWriterSession;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.commons.util.Sha1Digest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class ContentWriterServiceTestIT {
    static ContentWriterClient contentWriterClient;

    static ConfigAdapter db;

    static RethinkDB r = RethinkDB.r;

    @BeforeClass
    public static void init() throws DbConnectionException {
        String contentWriterHost = System.getProperty("contentwriter.host");
        int contentWriterPort = Integer.parseInt(System.getProperty("contentwriter.port"));
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));
        System.out.println("Database address: " + dbHost + ":" + dbPort);

        contentWriterClient = new ContentWriterClient(contentWriterHost, contentWriterPort);

        if (!DbService.isConfigured()) {
            CommonSettings dbSettings = new CommonSettings()
                    .withDbHost(dbHost)
                    .withDbPort(dbPort)
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword("");
            DbService.configure(dbSettings);
        }
        db = DbService.getInstance().getConfigAdapter();
    }

    @AfterClass
    public static void shutdown() {
        contentWriterClient.close();
    }

    @Test
    public void write() throws StatusException, InterruptedException, DbException {
//        System.out.println(db.updateConfigObjects(UpdateRequest.newBuilder()
//                .setListRequest(ListRequest.newBuilder().setKind(Kind.collection).build())
//                .setUpdateMask(FieldMask.newBuilder().addPaths("collection.subCollections").build())
//                .build()));
//
//        try (ChangeFeed<ConfigObject> r = db.listConfigObjects(ListRequest.newBuilder().setKind(Kind.collection).build())) {
//            r.stream()
//                    .forEach(o -> System.out.println(o));
//        }

        ContentWriterSession session = contentWriterClient.createSession();
        System.out.println("Session open: " + session.isOpen());

        Sha1Digest blockDigest = new Sha1Digest();
        Sha1Digest payloadDigest = new Sha1Digest();
        Sha1Digest headerDigest;

        ByteString headerData = ByteString.copyFromUtf8("Jadda");

        updateDigest(headerData, blockDigest);
        headerDigest = blockDigest.clone();

        RecordMeta rMeta = RecordMeta.newBuilder()
                .setRecordNum(0)
                .setSize(headerData.size())
                .setBlockDigest(blockDigest.getPrefixedDigestString())
                .setPayloadDigest(payloadDigest.getPrefixedDigestString())
                .setType(RecordType.METADATA)
                .build();
        RecordMeta screenshotMeta = RecordMeta.newBuilder()
                .setRecordNum(1)
                .setSize(headerData.size())
                .setBlockDigest(blockDigest.getPrefixedDigestString())
                .setPayloadDigest(payloadDigest.getPrefixedDigestString())
                .setType(RecordType.RESOURCE)
                .setSubCollection(SubCollectionType.SCREENSHOT)
                .build();
        WriteRequestMeta meta = WriteRequestMeta.newBuilder()
                .setCollectionRef(ConfigRef.newBuilder().setKind(Kind.collection).setId("2fa23773-d7e1-4748-8ab6-9253e470a3f5"))
                .setIpAddress("127.0.0.1")
                .putRecordMeta(0, rMeta)
                .putRecordMeta(1, screenshotMeta)
                .build();
        session.sendMetadata(meta);

        Data header = Data.newBuilder()
                .setRecordNum(0)
                .setData(headerData)
                .build();
        session.sendHeader(header);

        Data screenshot = Data.newBuilder()
                .setRecordNum(1)
                .setData(headerData)
                .build();
        session.sendPayload(screenshot);

        System.out.println("End: " + session.finish());
        System.out.println("Session open: " + session.isOpen());

        WarcFileSet wfs = WarcInspector.getWarcFiles();
        wfs.listFiles().forEach(wf -> {
            System.out.println(wf.getName());
            wf.getContent().forEach(r -> System.out.println("  - " + r.header.contentTypeStr));
        });

        System.out.println("-----------");

        wfs = WarcInspector.getWarcFiles();
        wfs.listFiles().forEach(wf -> {
            System.out.println(wf.getName());
            wf.getContent().forEach(r -> {
                String content = "";
                if (r.hasPayload()) {
                    content = "\n    " + new BufferedReader(new InputStreamReader(r.getPayloadContent())).lines().collect(Collectors.joining("\n    "));
                }
                System.out.println("  - " + r.header.warcRecordIdStr);
                r.header.getHeaderList().forEach(h -> System.out.print("    " + new String(h.raw)));
                System.out.println(content + "\n");
            });
        });
        System.out.println("-----------");
    }

    private void updateDigest(ByteString buf, Sha1Digest... digests) {
        for (Sha1Digest d : digests) {
            d.update(buf);
        }
    }
}