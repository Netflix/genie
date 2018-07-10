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

package com.netflix.genie.web.rpc.grpc.services.impl.v4

import com.netflix.genie.proto.AgentHeartBeat
import com.netflix.genie.proto.ServerHeartBeat
import com.netflix.genie.web.services.AgentRoutingService
import io.grpc.stub.StreamObserver
import org.springframework.scheduling.TaskScheduler
import spock.lang.Specification

import java.util.concurrent.ScheduledFuture

class GrpcHeartBeatServiceImplSpec extends Specification {
    AgentRoutingService agentRoutingService
    GrpcHeartBeatServiceImpl service
    StreamObserver<ServerHeartBeat> responseObserver
    TaskScheduler taskScheduler
    ScheduledFuture taskFuture
    Runnable task

    void setup() {
        this.taskFuture = Mock(ScheduledFuture)
        this.taskScheduler = Mock(TaskScheduler) {
            1 * scheduleWithFixedDelay(_ as Runnable, _ as Long) >> {
                args ->
                    this.task = args[0] as Runnable
                return taskFuture
            }
        }
        this.agentRoutingService = Mock(AgentRoutingService)
        this.responseObserver = Mock(StreamObserver)
        this.service = new GrpcHeartBeatServiceImpl(agentRoutingService, taskScheduler)
        assert task != null
    }

    void cleanup() {
        if (this.service != null) {
            this.service.shutdown()
        }
    }

    def "Connect, heartbeat twice, disconnect"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        StreamObserver<ServerHeartBeat> responseObserver = Mock(StreamObserver)

        when:
        StreamObserver<AgentHeartBeat> requestObserver = service.heartbeat(responseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId("").build())

        then:
        0 * agentRoutingService.handleClientConnected(jobId)

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId).build())

        then:
        1 * agentRoutingService.handleClientConnected(jobId)

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId).build())

        then:
        0 * agentRoutingService.handleClientConnected(jobId)

        when:
        requestObserver.onCompleted()

        then:
        1 * agentRoutingService.handleClientDisconnected(jobId)
        1 * responseObserver.onCompleted()

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId).build())

        then:
        0 * agentRoutingService.handleClientConnected(jobId)

        when:
        requestObserver.onCompleted()

        then:
        0 * responseObserver.onCompleted()
        0 * agentRoutingService.handleClientDisconnected(_ as String)

        when:
        requestObserver.onError(new RuntimeException())

        then:
        0 * responseObserver.onError(_ as Exception)
        0 * agentRoutingService.handleClientDisconnected(_ as String)

        when:
        service.shutdown()

        then:
        1 * taskFuture.cancel(false)
        0 * responseObserver.onCompleted()
        0 * agentRoutingService.handleClientDisconnected(_ as String)
    }

    def "Connect, heartbeat, error"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        StreamObserver<ServerHeartBeat> responseObserver = Mock(StreamObserver)
        Exception e = new RuntimeException()

        when:
        StreamObserver<AgentHeartBeat> requestObserver = service.heartbeat(responseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId).build())

        then:
        1 * agentRoutingService.handleClientConnected(jobId)

        when:
        requestObserver.onError(e)

        then:
        1 * agentRoutingService.handleClientDisconnected(jobId)
        1 * responseObserver.onError(e)
    }

    def "Connect, disconnect"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        StreamObserver<ServerHeartBeat> responseObserver = Mock(StreamObserver)

        when:
        StreamObserver<AgentHeartBeat> requestObserver = service.heartbeat(responseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onCompleted()

        then:
        0 * agentRoutingService.handleClientDisconnected(jobId)
        1 * responseObserver.onCompleted()
    }

    def "Connect, error"() {
        setup:
        StreamObserver<ServerHeartBeat> responseObserver = Mock(StreamObserver)
        Exception e = new RuntimeException()

        when:
        StreamObserver<AgentHeartBeat> requestObserver = service.heartbeat(responseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onError(e)

        then:
        0 * agentRoutingService.handleClientConnected(_ as String)
        1 * responseObserver.onError(e)
    }

    def "Send server heartbeats and close streams on shutdown"() {
        setup:
        String jobId1 = UUID.randomUUID().toString()
        String jobId2 = UUID.randomUUID().toString()

        StreamObserver<ServerHeartBeat> responseObserver1 = Mock(StreamObserver)
        StreamObserver<ServerHeartBeat> responseObserver2 = Mock(StreamObserver)

        when:
        StreamObserver<AgentHeartBeat> requestObserver1 = service.heartbeat(responseObserver1)
        StreamObserver<AgentHeartBeat> requestObserver2 = service.heartbeat(responseObserver2)

        then:
        requestObserver1 != null
        requestObserver2 != null

        when:
        requestObserver1.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId1).build())
        requestObserver2.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId2).build())

        then:
        1 * agentRoutingService.handleClientConnected(jobId1)
        1 * agentRoutingService.handleClientConnected(jobId2)

        when:
        task.run()

        then:
        1 * responseObserver1.onNext(_ as ServerHeartBeat)
        1 * responseObserver2.onNext(_ as ServerHeartBeat)

        when:
        service.shutdown()
        service = null

        then:
        1 * taskFuture.cancel(false)
        1 * agentRoutingService.handleClientDisconnected(jobId1)
        1 * agentRoutingService.handleClientDisconnected(jobId2)
        1 * responseObserver1.onCompleted()
        1 * responseObserver2.onCompleted()
    }


    def "Accept anonymous agents"() {
        setup:
        String jobId = ""
        StreamObserver<ServerHeartBeat> responseObserver = Mock(StreamObserver)

        when:
        StreamObserver<AgentHeartBeat> requestObserver = service.heartbeat(responseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId).build())

        then:
        0 * agentRoutingService.handleClientConnected(jobId)

        when:
        task.run()

        then:
        1 * responseObserver.onNext(_ as ServerHeartBeat)

        when:
        requestObserver.onCompleted()

        then:
        1 * responseObserver.onCompleted()
    }

}
