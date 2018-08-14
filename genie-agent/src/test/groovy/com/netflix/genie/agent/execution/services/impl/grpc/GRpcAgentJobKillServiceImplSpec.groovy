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

import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.execution.services.impl.KillServiceImpl
import com.netflix.genie.proto.JobKillRegistrationRequest
import com.netflix.genie.proto.JobKillRegistrationResponse
import com.netflix.genie.proto.JobKillServiceGrpc
import com.netflix.genie.test.categories.UnitTest
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.springframework.core.task.TaskExecutor
import spock.lang.Specification

@Category(UnitTest.class)
class GRpcAgentJobKillServiceImplSpec extends Specification {

    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    String jobId
    GRpcAgentJobKillServiceImpl service
    JobKillServiceGrpc.JobKillServiceFutureStub client;
    KillService killService
    TaskExecutor killExecutor
    TestJobKillService server

    void setup() {
        server = new TestJobKillService()
        grpcServerRule.getServiceRegistry().addService(server)
        jobId = UUID.randomUUID().toString()
        client = JobKillServiceGrpc.newFutureStub(grpcServerRule.getChannel())
        killService = Mock(KillService)
        killExecutor = Mock(TaskExecutor)
        service = new GRpcAgentJobKillServiceImpl(client, killService, killExecutor)
    }

    void cleanup() {
        service.stop()
        grpcServerRule.getChannel().shutdownNow()
    }

    def "Starting service more than once throws Exception"() {

        JobKillRegistrationRequest request =
            JobKillRegistrationRequest.newBuilder().setJobId(jobId).build()

        when:
        service.start(jobId)

        then:
        noExceptionThrown()

        when:
        service.start(jobId)

        then:
        thrown(IllegalStateException)
    }

    def "Successfully handle kill notification from the service"() {

        Runnable killCallback

        when:
        service.start(jobId)

        then:
        noExceptionThrown()

        when:
        server.sendKill()

        then:
        1 * killExecutor.execute(_ as Runnable) >> {
            args -> killCallback = args[0]
        }

        when:
        killCallback.run()

        then:
        1 * killService.kill(KillService.KillSource.API_KILL_REQUEST)
    }

    def "Failure registering with the service followed by successful reregistration and kill notification"() {
        Runnable killCallback

        when:
        service.start(jobId)

        then:
        noExceptionThrown()

        when:
        server.sendError()

        then:
        1 * killExecutor.execute(_ as Runnable) >> {
            args -> killCallback = args[0]
        }

        when:
        killCallback.run()

        then:
        0 * killService.kill(_)

        when:
        server.sendKill()

        then: "onSuccess callback gets invoked"
        1 * killExecutor.execute(_ as Runnable) >> {
            args -> killCallback = args[0]
        }

        when:
        killCallback.run()

        then:
        1 * killService.kill(KillService.KillSource.API_KILL_REQUEST)
    }

    /**
     * Mock service for testing.
     */
    private class TestJobKillService extends JobKillServiceGrpc.JobKillServiceImplBase {

        StreamObserver<JobKillRegistrationResponse> killResponseObserver

        @Override
        void registerForKillNotification(
                final JobKillRegistrationRequest request,
                final StreamObserver<JobKillRegistrationResponse> responseObserver
        ) {
            killResponseObserver = responseObserver
        }

        void sendError() {
            killResponseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE))
        }

        void sendKill() {
            killResponseObserver.onNext(
                JobKillRegistrationResponse.newBuilder().build()
            )
            killResponseObserver.onCompleted()
        }
    }
}
