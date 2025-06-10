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

import com.google.common.util.concurrent.SettableFuture
import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.properties.JobKillServiceProperties
import com.netflix.genie.common.internal.util.ExponentialBackOffTrigger
import com.netflix.genie.proto.JobKillRegistrationRequest
import com.netflix.genie.proto.JobKillRegistrationResponse
import com.netflix.genie.proto.JobKillServiceGrpc
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.junit.Rule
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.Trigger
import spock.lang.Specification

import java.util.concurrent.ScheduledFuture

class GRpcAgentJobKillServiceImplSpec extends Specification {

    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    String jobId
    GRpcAgentJobKillServiceImpl service
    JobKillServiceGrpc.JobKillServiceFutureStub client
    KillService killService
    TaskScheduler taskScheduler
    ScheduledFuture<?> periodicTaskScheduledFuture
    TestJobKillService testServer
    JobKillServiceProperties serviceProperties
    SettableFuture<JobKillRegistrationResponse> futureResponse

    void setup() {
        this.jobId = UUID.randomUUID().toString()
        this.testServer = new TestJobKillService()
        this.grpcServerRule.getServiceRegistry().addService(testServer)
        this.client = JobKillServiceGrpc.newFutureStub(grpcServerRule.getChannel())
        this.killService = Mock(KillService)
        this.taskScheduler = Mock(TaskScheduler)
        this.periodicTaskScheduledFuture = Mock(ScheduledFuture)
        this.serviceProperties = new JobKillServiceProperties()
        this.service = new GRpcAgentJobKillServiceImpl(client, killService, taskScheduler, serviceProperties)
    }

    void cleanup() {
        grpcServerRule.getChannel().shutdownNow()
    }

    def "Run without kill or errors"() {
        Runnable capturedTask
        Trigger capturedTrigger

        when:
        service.start(jobId)

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Trigger) >> { args ->
            capturedTask = args[0] as Runnable
            capturedTrigger = args[1] as Trigger
            return periodicTaskScheduledFuture
        }
        capturedTask != null
        capturedTrigger != null
        testServer.receivedRequests.size() == 1
        testServer.receivedRequests[0].jobId == jobId

        when:
        capturedTask.run()

        then:
        noExceptionThrown()
        testServer.receivedRequests.size() == 1 // No new request yet since the first one is still pending

        when:
        service.stop()

        then:
        1 * periodicTaskScheduledFuture.cancel(true)
    }

    def "Kill"() {
        Runnable capturedTask
        ExponentialBackOffTrigger capturedTrigger

        when:
        service.start(jobId)

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as ExponentialBackOffTrigger) >> { args ->
            capturedTask = args[0] as Runnable
            capturedTrigger = args[1] as ExponentialBackOffTrigger
            return periodicTaskScheduledFuture
        }
        capturedTask != null
        capturedTrigger != null
        testServer.receivedRequests.size() == 1
        testServer.receivedRequests[0].jobId == jobId

        when:
        // Complete the pending request with success
        testServer.completeAllPendingRequests()

        then:
        1 * killService.kill(KillService.KillSource.API_KILL_REQUEST)
        testServer.receivedRequests.size() == 1
    }

    def "Error"() {
        Runnable capturedTask
        ExponentialBackOffTrigger capturedTrigger

        when:
        service.start(jobId)

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as ExponentialBackOffTrigger) >> { args ->
            capturedTask = args[0] as Runnable
            capturedTrigger = args[1] as ExponentialBackOffTrigger
            return periodicTaskScheduledFuture
        }
        capturedTask != null
        capturedTrigger != null
        testServer.receivedRequests.size() == 1
        testServer.receivedRequests[0].jobId == jobId

        when:
        // Fail the pending request with an error
        testServer.failAllPendingRequests(new RuntimeException("Test error"))
        capturedTask.run()

        then:
        0 * killService.kill(_)
        testServer.receivedRequests.size() == 2  // A new request should be created after the previous one failed
    }

    /**
     * Test implementation of JobKillService that tracks requests and allows controlling responses
     */
    private static class TestJobKillService extends JobKillServiceGrpc.JobKillServiceImplBase {
        List<JobKillRegistrationRequest> receivedRequests = []
        List<StreamObserver<JobKillRegistrationResponse>> pendingObservers = []

        @Override
        void registerForKillNotification(
            JobKillRegistrationRequest request,
            StreamObserver<JobKillRegistrationResponse> responseObserver
        ) {
            receivedRequests.add(request)
            pendingObservers.add(responseObserver)
        }

        void completeAllPendingRequests() {
            JobKillRegistrationResponse response = JobKillRegistrationResponse.newBuilder().build()
            pendingObservers.each { observer ->
                observer.onNext(response)
                observer.onCompleted()
            }
            pendingObservers.clear()
        }

        void failAllPendingRequests(Throwable error) {
            pendingObservers.each { observer ->
                observer.onError(error)
            }
            pendingObservers.clear()
        }
    }
}
