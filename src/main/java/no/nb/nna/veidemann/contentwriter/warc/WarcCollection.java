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

package no.nb.nna.veidemann.contentwriter.warc;

import no.nb.nna.veidemann.api.config.v1.Collection.RotationPolicy;
import no.nb.nna.veidemann.api.config.v1.Collection.SubCollection;
import no.nb.nna.veidemann.api.config.v1.Collection.SubCollectionType;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.commons.util.Pool.Lease;
import no.nb.nna.veidemann.contentwriter.ContentWriter;
import no.nb.nna.veidemann.contentwriter.settings.Settings;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.Map;

public class WarcCollection implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(WarcCollection.class);

    static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern("YYYYMMddHH");
    static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern("YYYYMMdd");
    static final DateTimeFormatter MONTH_FORMAT = DateTimeFormatter.ofPattern("YYYYMM");
    static final DateTimeFormatter YEAR_FORMAT = DateTimeFormatter.ofPattern("YYYY");

    final ConfigObject config;
    final WarcWriterPool warcWriterPool;
    final Map<SubCollectionType, WarcWriterPool> subCollections;
    final String filePrefix;
    final String currentFileRotationKey;
    final Settings settings = ContentWriter.getSettings();

    public WarcCollection(ConfigObject config) {

        this.config = config;
        filePrefix = createFilePrefix(ProtoUtils.getNowOdt());
        currentFileRotationKey = createFileRotationKey(config.getCollection().getFileRotationPolicy(), ProtoUtils.getNowOdt());

        this.warcWriterPool = new WarcWriterPool(
                config,
                null,
                filePrefix,
                new File(settings.getWarcDir()),
                config.getCollection().getFileSize(),
                config.getCollection().getCompress(),
                settings.getWarcWriterPoolSize(),
                settings.getHostName());

        subCollections = new EnumMap<>(SubCollectionType.class);
        for (SubCollection sub : config.getCollection().getSubCollectionsList()) {
            subCollections.put(sub.getType(), new WarcWriterPool(
                    config,
                    sub,
                    filePrefix + "_" + sub.getName(),
                    new File(settings.getWarcDir()),
                    config.getCollection().getFileSize(),
                    config.getCollection().getCompress(),
                    settings.getWarcWriterPoolSize(),
                    settings.getHostName()));
        }
    }

    public Instance getWarcWriters() {
        return new Instance();
    }

    public String getCollectionName(SubCollectionType subType) {
        return subCollections.getOrDefault(subType, warcWriterPool).getName();
    }

    public boolean shouldFlushFiles(ConfigObject config, OffsetDateTime timestamp) {
        if (!createFileRotationKey(config.getCollection().getFileRotationPolicy(), timestamp)
                .equals(currentFileRotationKey)) {
            return true;
        }

        if (!createFilePrefix(timestamp).equals(filePrefix)) {
            return true;
        }

        if (config == this.config) {
            return false;
        } else {
            ConfigObject c = this.config;
            ConfigObject other = config;
            boolean isEqual = true;
            isEqual = isEqual && c.hasMeta() == other.hasMeta();
            isEqual = isEqual && c.getMeta().getName().equals(other.getMeta().getName());
            isEqual = isEqual && c.getMeta().getDescription().equals(other.getMeta().getDescription());

            isEqual = isEqual && c.getCollection().getCollectionDedupPolicy() == other.getCollection().getCollectionDedupPolicy();
            isEqual = isEqual && c.getCollection().getFileRotationPolicy() == other.getCollection().getFileRotationPolicy();
            isEqual = isEqual && c.getCollection().getCompress() == other.getCollection().getCompress();
            isEqual = isEqual && c.getCollection().getFileSize() == other.getCollection().getFileSize();
            isEqual = isEqual && c.getCollection().getSubCollectionsList().equals(other.getCollection().getSubCollectionsList());

            return !isEqual;
        }
    }

    String createFilePrefix(OffsetDateTime timestamp) {
        String name = config.getMeta().getName();
        String dedupRotationKey = createFileRotationKey(config.getCollection().getCollectionDedupPolicy(), timestamp);
        if (dedupRotationKey.isEmpty()) {
            return name;
        } else {
            return name + "_" + dedupRotationKey;
        }
    }

    String createFileRotationKey(RotationPolicy fileRotationPolicy, OffsetDateTime timestamp) {
        switch (fileRotationPolicy) {
            case HOURLY:
                return timestamp.format(HOUR_FORMAT);
            case DAILY:
                return timestamp.format(DAY_FORMAT);
            case MONTHLY:
                return timestamp.format(MONTH_FORMAT);
            case YEARLY:
                return timestamp.format(YEAR_FORMAT);
            default:
                return "";
        }
    }

    @Override
    public void close() {
        try {
            warcWriterPool.close();
        } catch (InterruptedException e) {
            LOG.error("Failed closing collection " + warcWriterPool.getName(), e);
        }
        for (WarcWriterPool sub : subCollections.values()) {
            try {
                sub.close();
            } catch (InterruptedException e) {
                LOG.error("Failed closing collection " + sub.getName(), e);
            }
        }
    }

    public void deleteFiles() throws IOException {
        Path dir = Paths.get(settings.getWarcDir());
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, warcWriterPool.getName() + "*.warc*")) {
            for (Path path : stream) {
                LOG.info("Deleting " + path);
                Files.delete(path);
            }
        }
    }

    public class Instance implements AutoCloseable {
        Lease<SingleWarcWriter> warcWriterLease;
        final Map<SubCollectionType, Lease<SingleWarcWriter>> subCollectionWarcWriterLeases =
                new EnumMap<>(SubCollectionType.class);

        public SingleWarcWriter getWarcWriter(SubCollectionType subType) {
            if (subCollections.containsKey(subType)) {
                Lease<SingleWarcWriter> sub = subCollectionWarcWriterLeases.computeIfAbsent(subType, k -> {
                    try {
                        return subCollections.get(k).lease();
                    } catch (InterruptedException e) {
                        LOG.error("Can't get WarcWriter", e);
                        throw new RuntimeException(e);
                    }
                });
                return sub.getObject();
            } else {
                if (warcWriterLease == null) {
                    try {
                        warcWriterLease = warcWriterPool.lease();
                    } catch (InterruptedException e) {
                        LOG.error("Can't get WarcWriter", e);
                        throw new RuntimeException(e);
                    }
                }
                return warcWriterLease.getObject();
            }
        }

        @Override
        public void close() {
            if (warcWriterLease != null) {
                warcWriterLease.close();
            }
            for (Lease<SingleWarcWriter> sub : subCollectionWarcWriterLeases.values()) {
                sub.close();
            }
        }
    }
}
