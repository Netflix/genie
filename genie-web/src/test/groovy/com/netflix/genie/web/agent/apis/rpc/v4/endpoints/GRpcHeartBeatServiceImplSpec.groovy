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
package com.netflix.genie.web.agent.apis.rpc.v4.endpoints

import com.netflix.genie.proto.AgentHeartBeat
import com.netflix.genie.proto.ServerHeartBeat
import com.netflix.genie.web.agent.services.AgentConnectionTrackingService
import com.netflix.genie.web.properties.HeartBeatProperties
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.scheduling.TaskScheduler
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.ScheduledFuture

class GRpcHeartBeatServiceImplSpec extends Specification {
    AgentConnectionTrackingService agentConnectionTrackingService
    GRpcHeartBeatServiceImpl service
    StreamObserver<ServerHeartBeat> responseObserver
    TaskScheduler taskScheduler
    ScheduledFuture taskFuture
    Runnable task
    HeartBeatProperties props

    void setup() {
        this.taskFuture = Mock(ScheduledFuture)
        this.taskScheduler = Mock(TaskScheduler) {
            scheduleWithFixedDelay(_ as Runnable, _ as Duration) >> {
                args ->
                    this.task = args[0] as Runnable
                    return taskFuture
            }
        }
        this.agentConnectionTrackingService = Mock(AgentConnectionTrackingService)
        this.responseObserver = Mock(StreamObserver)
        this.props = new HeartBeatProperties()
        this.service = new GRpcHeartBeatServiceImpl(agentConnectionTrackingService, props, taskScheduler, new SimpleMeterRegistry())
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
        String streamId

        when:
        StreamObserver<AgentHeartBeat> requestObserver = service.heartbeat(responseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId("").build())

        then:
        0 * agentConnectionTrackingService._

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId).build())

        then:
        1 * agentConnectionTrackingService.notifyHeartbeat(_ as String, jobId) >> {
            args ->
                streamId = (args[0] as String)
        }
        streamId != null

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId).build())

        then:
        1 * agentConnectionTrackingService.notifyHeartbeat(streamId, jobId)

        when:
        requestObserver.onCompleted()

        then:
        1 * agentConnectionTrackingService.notifyDisconnected(streamId as String, jobId)
        1 * responseObserver.onCompleted()

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId).build())

        then:
        0 * agentConnectionTrackingService._

        when:
        requestObserver.onCompleted()

        then:
        0 * responseObserver.onCompleted()
        0 * agentConnectionTrackingService._

        when:
        requestObserver.onError(new RuntimeException())

        then:
        0 * responseObserver.onError(_ as Exception)
        0 * agentConnectionTrackingService._

        when:
        service.shutdown()

        then:
        1 * taskFuture.cancel(false)
        0 * responseObserver.onCompleted()
        0 * agentConnectionTrackingService._
    }

    def "Connect, heartbeat, error"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        StreamObserver<ServerHeartBeat> responseObserver = Mock(StreamObserver)
        Exception e = new RuntimeException()
        String streamId

        when:
        StreamObserver<AgentHeartBeat> requestObserver = service.heartbeat(responseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId).build())

        then:
        1 * agentConnectionTrackingService.notifyHeartbeat(_ as String, jobId) >> {
            args ->
                streamId = args[0] as String
        }
        streamId != null

        when:
        requestObserver.onError(e)

        then:
        1 * agentConnectionTrackingService.notifyDisconnected(streamId, jobId)
        0 * responseObserver.onCompleted()
    }


    def "Connect, heartbeat, server-side error"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        StreamObserver<ServerHeartBeat> responseObserver1 = Mock(StreamObserver)
        StreamObserver<ServerHeartBeat> responseObserver2 = Mock(StreamObserver)


        String streamId1
        String streamId2

        when:
        StreamObserver<AgentHeartBeat> requestObserver1 = service.heartbeat(responseObserver1)
        StreamObserver<AgentHeartBeat> requestObserver2 = service.heartbeat(responseObserver2)

        then:
        requestObserver1 != null
        requestObserver2 != null

        when:
        requestObserver1.onNext(AgentHeartBeat.newBuilder().setClaimedJobId(jobId).build())
        requestObserver2.onNext(AgentHeartBeat.newBuilder().build())

        then:
        1 * agentConnectionTrackingService.notifyHeartbeat(_ as String, jobId) >> {
            args ->
                streamId1 = args[0] as String
        }
        streamId1 != null

        when:
        task.run()

        then:
        1 * responseObserver1.onNext(_) >> { throw new StatusRuntimeException(Status.CANCELLED) }
        1 * responseObserver2.onNext(_) >> { throw new IllegalStateException() }
        1 * agentConnectionTrackingService.notifyDisconnected(streamId1, jobId)
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
        0 * agentConnectionTrackingService._
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
        0 * agentConnectionTrackingService._
        0 * responseObserver.onCompleted()
    }

    def "Send server heartbeats and close streams on shutdown"() {
        setup:
        String jobId1 = UUID.randomUUID().toString()
        String jobId2 = UUID.randomUUID().toString()
        String streamId1
        String streamId2

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
        1 * agentConnectionTrackingService.notifyHeartbeat(_ as String, jobId1) >> {
            args ->
                streamId1 = args[0] as String
        }
        1 * agentConnectionTrackingService.notifyHeartbeat(_ as String, jobId2) >> {
            args ->
                streamId2 = args[0] as String
        }
        streamId1 != null
        streamId2 != null

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
        1 * agentConnectionTrackingService.notifyDisconnected(streamId1, jobId1)
        1 * agentConnectionTrackingService.notifyDisconnected(streamId2, jobId2)
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
        0 * agentConnectionTrackingService._

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
