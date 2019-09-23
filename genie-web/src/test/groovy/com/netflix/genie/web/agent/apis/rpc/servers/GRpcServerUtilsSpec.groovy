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
package com.netflix.genie.web.agent.apis.rpc.servers

import io.grpc.Server
import spock.lang.Specification

/**
 * Specifications for {@link GRpcServerUtils}.
 *
 * @author tgianos
 */
class GRpcServerUtilsSpec extends Specification {

    def "Can start server"() {
        def server = Mock(Server)
        def port = 28_283

        when:
        def startedPort = GRpcServerUtils.startServer(server)

        then:
        1 * server.start()
        1 * server.getPort() >> port
        startedPort == port
    }

    def "Can't start server"() {
        def server = Mock(Server)
        def port = 28_381

        when:
        def startedPort = GRpcServerUtils.startServer(server)

        then:
        1 * server.start() >> {
            throw new IllegalStateException("already started")
        }
        1 * server.getPort() >> port
        startedPort == port

        when:
        GRpcServerUtils.startServer(server)

        then:
        1 * server.start() >> {
            throw new IOException("Port already in use")
        }
        1 * server.getPort() >> port
        thrown(IllegalStateException)
    }
}
