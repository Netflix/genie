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

import java.io.IOException;

/**
 * Utilities for working with a gRPC {@link Server} instance.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
@SuppressWarnings("FinalClass")
public class GRpcServerUtils {

    /**
     * Utility class.
     */
    private GRpcServerUtils() {
    }

    /**
     * Attempt to start a gRpc server on this node.
     *
     * @param server The server to start
     * @return The port the server is running on.
     * @throws IllegalStateException If we're unable to start the server
     */
    public static int startServer(final Server server) throws IllegalStateException {
        try {
            server.start();
            final int port = server.getPort();
            log.info("Successfully started gRPC server on port {}", port);
            return port;
        } catch (final IllegalStateException ise) {
            final int port = server.getPort();
            log.info("Server already started on port {}", port);
            return port;
        } catch (final IOException ioe) {
            throw new IllegalStateException("Unable to start gRPC server on port " + server.getPort(), ioe);
        }
    }
}
