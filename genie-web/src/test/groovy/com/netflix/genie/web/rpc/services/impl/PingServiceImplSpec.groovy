/*
 *
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.genie.web.rpc.services.impl

import com.google.protobuf.util.Timestamps
import com.netflix.genie.proto.PingRequest
import com.netflix.genie.proto.PongResponse
import com.netflix.genie.test.categories.UnitTest
import io.grpc.stub.StreamObserver
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest)
class PingServiceImplSpec extends Specification {
    public static final String HOST_NAME = "genie123.netflix.com"

    StreamObserver<PongResponse> responseObserver

    void setup() {
        responseObserver = Mock()
    }

    void cleanup() {
    }

    def "Ping"() {
        setup:
        def service = new PingServiceImpl(HOST_NAME)
        String requestId = UUID.randomUUID().toString()
        PingRequest pingRequest = PingRequest.newBuilder()
                .setRequestId(requestId)
                .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .build()
        PongResponse pongResponse = null

        when:
        service.ping(pingRequest, responseObserver)

        then:
        1 * responseObserver.onNext(_ as PongResponse) >> {
            response -> pongResponse = response[0]
        }
        1 * responseObserver.onCompleted()

        expect:
        pongResponse != null
        HOST_NAME == pongResponse.getServerMetadataOrThrow(PingServiceImpl.ServerMetadataKeys.SERVER_NAME)
        requestId == pongResponse.getRequestId()
        null != pongResponse.getTimestamp()
    }
}
