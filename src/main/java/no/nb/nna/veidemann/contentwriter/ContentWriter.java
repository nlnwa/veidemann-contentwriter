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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.opentracing.TracerFactory;
import no.nb.nna.veidemann.contentwriter.settings.Settings;
import no.nb.nna.veidemann.contentwriter.text.TextExtractor;
import no.nb.nna.veidemann.contentwriter.warc.WarcCollectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for launching the service.
 */
public class ContentWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ContentWriter.class);

    private static final Settings SETTINGS;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

        TracerFactory.init("ContentWriter");
    }

    /**
     * Create a new ContentWriter service.
     */
    public ContentWriter() {
    }

    /**
     * Start the service.
     * <p>
     *
     * @return this instance
     */
    public ContentWriter start() {
        try (DbService db = DbService.configure(SETTINGS);
             WarcCollectionRegistry warcCollectionRegistry = new WarcCollectionRegistry();
             TextExtractor textExtractor = new TextExtractor();
             ApiServer apiServer = new ApiServer(SETTINGS.getApiPort(), SETTINGS.getTerminationGracePeriodSeconds(), warcCollectionRegistry, textExtractor);) {

            registerShutdownHook();

            apiServer.start();

            LOG.info("Veidemann Content Writer (v. {}) started",
                    ContentWriter.class.getPackage().getImplementationVersion());

            try {
                Thread.currentThread().join();
            } catch (InterruptedException ex) {
                // Interrupted, shut down
            }
        } catch (ConfigException | DbException ex) {
            LOG.error("Configuration error: {}", ex.getLocalizedMessage());
            System.exit(1);
        }

        return this;
    }

    private void registerShutdownHook() {
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down since JVM is shutting down");

            mainThread.interrupt();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                //
            }
            System.err.println("*** gracefully shut down");

        }));
    }

    /**
     * Get the settings object.
     * <p>
     *
     * @return the settings
     */
    public static Settings getSettings() {
        return SETTINGS;
    }

}
