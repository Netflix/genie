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
package com.netflix.genie.web.rpc.grpc.servers

import io.grpc.Server
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Specifications for {@link GRpcServerManager}.
 *
 * @author tgianos
 */
class GRpcServerManagerSpec extends Specification {

    def "can start server"() {
        def server = Mock(Server) {
            getPort() >> 9090
        }

        when:
        new GRpcServerManager(server)

        then:
        1 * server.start()
        noExceptionThrown()

        when:
        new GRpcServerManager(server)

        then:
        1 * server.start() >> {
            throw new IllegalStateException("Server already started")
        }
        noExceptionThrown()

        when:
        new GRpcServerManager(server)

        then:
        1 * server.start() >> {
            throw new IOException("Port in use")
        }
        thrown(IllegalStateException)
    }

    def "can stop server"() {
        def server = Mock(Server) {
            getPort() >> 9090
        }

        when:
        def manager = new GRpcServerManager(server)

        then:
        1 * server.start()

        when:
        manager.close()

        then:
        1 * server.isShutdown() >> false
        1 * server.shutdownNow()
        1 * server.awaitTermination(_ as Long, _ as TimeUnit) >> true

        when:
        manager.close()

        then:
        1 * server.isShutdown() >> true
        0 * server.shutdownNow()
        1 * server.awaitTermination(_ as Long, _ as TimeUnit) >> false

        when:
        manager.close()

        then:
        1 * server.isShutdown() >> true
        0 * server.shutdownNow()
        1 * server.awaitTermination(_ as Long, _ as TimeUnit) >> {
            throw new InterruptedException("Interrupted")
        }
        noExceptionThrown()
    }
}
