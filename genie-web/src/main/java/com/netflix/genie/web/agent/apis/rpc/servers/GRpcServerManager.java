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
package com.netflix.genie.web.agent.apis.rpc.servers;

import io.grpc.Server;
import lombok.extern.slf4j.Slf4j;

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
     * @throws IllegalStateException If the server can't be started
     */
    public GRpcServerManager(final Server server) throws IllegalStateException {
        this.server = server;
        GRpcServerUtils.startServer(this.server);
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

    /**
     * Get the port the gRPC {@link Server} is listening on.
     *
     * @return The port or -1 if the server isn't currently listening on any port.
     */
    public int getServerPort() {
        // Note: Since you can't construct one of these managers without it starting the server the
        //       IllegalStateException defined in getPort() of the server shouldn't be possible here so it's not
        //       documented as being thrown
        return this.server.getPort();
    }
}
