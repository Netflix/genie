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

package com.netflix.genie.agent.execution.services.impl.grpc

import com.netflix.genie.proto.AgentHeartBeat
import com.netflix.genie.proto.HeartBeatServiceGrpc
import com.netflix.genie.proto.ServerHeartBeat
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.assertj.core.util.Lists
import org.junit.Rule
import org.springframework.scheduling.TaskScheduler
import spock.lang.Specification

import java.time.Instant
import java.util.concurrent.ScheduledFuture

class GrpcAgentHeartBeatServiceImplSpec extends Specification {

    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    String jobId
    HeartBeatServiceGrpc.HeartBeatServiceStub client
    TaskScheduler taskScheduler
    ScheduledFuture heartBeatFuture
    GrpcAgentHeartBeatServiceImpl service

    StreamObserver<ServerHeartBeat> currentResponseObserver
    StreamObserver<AgentHeartBeat> currentRequestObserver
    final List<AgentHeartBeat> heartbeatsReceived = Lists.newArrayList()


    void setup() {
        this.jobId = UUID.randomUUID().toString()
        this.taskScheduler = Mock(TaskScheduler)
        this.heartBeatFuture = Mock(ScheduledFuture)
        this.grpcServerRule.getServiceRegistry().addService(new TestService())
        this.client = HeartBeatServiceGrpc.newStub(grpcServerRule.getChannel())
        this.service = new GrpcAgentHeartBeatServiceImpl(client, taskScheduler)
        this.heartbeatsReceived.clear()
    }

    void cleanup() {
        this.service.stop()
    }

    def "Start, stop"() {
        when:
        service.start(jobId)

        then:
        noExceptionThrown()

        when:
        service.stop()

        then:
        noExceptionThrown()
    }

    def "Reset stream after error"() {
        Runnable sendHeartBeatsRunnable
        Runnable resetRunnable

        when:
        service.start(jobId)

        then:
        1 * taskScheduler.scheduleAtFixedRate(_ as Runnable, _ as Long) >> {
            args ->
                sendHeartBeatsRunnable = args[0] as Runnable
                return heartBeatFuture
        }
        sendHeartBeatsRunnable != null

        when:
        sendHeartBeatsRunnable.run()

        then:
        assert !heartbeatsReceived.isEmpty()
        assert service.isConnected()

        when:
        heartbeatsReceived.clear()
        currentResponseObserver.onError(new RuntimeException())

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            args ->
                resetRunnable = args[0] as Runnable
                return null
        }
        resetRunnable != null
        !service.isConnected()

        when:
        resetRunnable.run()
        sendHeartBeatsRunnable.run()

        then:
        !heartbeatsReceived.isEmpty()
        service.isConnected()
    }

    def "Double start"() {
        when:
        service.start(jobId)

        then:
        noExceptionThrown()

        when:
        service.start(jobId)

        then:
        thrown(IllegalStateException)
    }

    class TestService extends HeartBeatServiceGrpc.HeartBeatServiceImplBase {

        @Override
        StreamObserver<AgentHeartBeat> heartbeat(final StreamObserver<ServerHeartBeat> responseObserver) {
            currentResponseObserver = responseObserver
            currentRequestObserver = new StreamObserver<AgentHeartBeat>() {

                @Override
                void onNext(final AgentHeartBeat value) {
                    synchronized (heartbeatsReceived) {
                        heartbeatsReceived.add(value)
                        heartbeatsReceived.notifyAll()
                    }

                    currentResponseObserver.onNext(ServerHeartBeat.getDefaultInstance())
                }

                @Override
                void onError(final Throwable t) {
                    currentResponseObserver.onError()

                }

                @Override
                void onCompleted() {
                    currentResponseObserver.onCompleted()
                }
            }

            return currentRequestObserver
        }
    }
}
