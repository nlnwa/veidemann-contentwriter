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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opentracing.contrib.ServerTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.contentwriter.text.TextExtractor;
import no.nb.nna.veidemann.contentwriter.warc.WarcCollectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ApiServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ApiServer.class);
    private final Server server;
    private final ExecutorService threadPool;
    private int shutdownTimeoutSeconds = 60;


    /**
     * Construct a new REST API server.
     */
    public ApiServer(int port, int shutdownTimeoutSeconds, WarcCollectionRegistry warcCollectionRegistry, TextExtractor textExtractor) {
        this(ServerBuilder.forPort(port), warcCollectionRegistry, textExtractor);
        this.shutdownTimeoutSeconds = shutdownTimeoutSeconds;
    }

    public ApiServer(ServerBuilder<?> serverBuilder, WarcCollectionRegistry warcCollectionRegistry, TextExtractor textExtractor) {

        ServerTracingInterceptor tracingInterceptor = new ServerTracingInterceptor.Builder(GlobalTracer.get())
                .withTracedAttributes(ServerTracingInterceptor.ServerRequestAttribute.CALL_ATTRIBUTES,
                        ServerTracingInterceptor.ServerRequestAttribute.METHOD_TYPE)
                .build();

        serverBuilder.intercept(tracingInterceptor);

        threadPool = Executors.newCachedThreadPool();
        serverBuilder.executor(threadPool);

        server = serverBuilder.addService(new ContentWriterService(warcCollectionRegistry, textExtractor)).build();
    }

    public ApiServer start() {
        try {
            server.start();

            LOG.info("Content Writer api listening on {}", server.getPort());

            return this;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void close() {
        long startTime = System.currentTimeMillis();
        server.shutdown();
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            server.shutdownNow();
        }
        threadPool.shutdown();
        long timeoutSeconds = shutdownTimeoutSeconds - ((System.currentTimeMillis() - startTime) / 1000);
        try {
            threadPool.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
        }
    }

}
