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

import no.nb.nna.veidemann.api.config.v1.Collection.SubCollection;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.commons.util.Pool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 */
public class WarcWriterPool extends Pool<SingleWarcWriter> {
    private static final Logger LOG = LoggerFactory.getLogger(WarcWriterPool.class);

    private final String name;

    /**
     * Creates the pool.
     * <p>
     *
     * @param poolSize maximum number of writers residing in the pool
     */
    public WarcWriterPool(final ConfigObject config, SubCollection subCollection, final String name, final File targetDir, final long maxFileSize,
                          final boolean compress, final int poolSize, final String hostName) {

        super(poolSize,
                () -> new SingleWarcWriter(config, subCollection, name, targetDir, hostName),
                null,
                singleWarcWriter -> {
                    try {
                        singleWarcWriter.close();
                    } catch (Exception e) {
                        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                        System.err.println("Failed closing collection " + name + ": " + e.getLocalizedMessage());
                    }
                });

        this.name = name;
        targetDir.mkdirs();
    }

    public String getName() {
        return name;
    }
}
