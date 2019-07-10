/*
 *
 *  Copyright 2019 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.web.rpc.grpc.servers;

import io.grpc.Server;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper around a {@link Server} instance which implements {@link AutoCloseable} to control startup and shutdown
 * along with the application.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class GRpcServerManager implements AutoCloseable {

    private final Server server;

    /**
     * Constructor.
     *
     * @param server The {@link Server} instance to manage
     */
    public GRpcServerManager(final Server server) {
        this.server = server;

        try {
            this.server.start();
            log.info("Successfully started gRPC server on port {}", this.server.getPort());
        } catch (final IllegalStateException ise) {
            log.info("Server already started on port {}", this.server.getPort());
        } catch (final IOException ioe) {
            throw new IllegalStateException("Unable to start gRPC server on port " + this.server.getPort(), ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!this.server.isShutdown()) {
            this.server.shutdownNow();
        }

        try {
            if (this.server.awaitTermination(30L, TimeUnit.SECONDS)) {
                log.info("Successfully shut down the gRPC server");
            } else {
                log.error("Unable to successfully shutdown the gRPC server in time allotted");
            }
        } catch (final InterruptedException ie) {
            log.error("Unable to shutdown gRPC server due to being interrupted", ie);
        }
    }
}
