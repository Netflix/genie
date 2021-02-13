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
    JobKillServiceGrpc.JobKillServiceFutureStub client;
    KillService killService
    TaskScheduler taskScheduler
    ScheduledFuture<?> periodicTaskScheduledFuture
    JobKillServiceGrpc.JobKillServiceImplBase server
    JobKillServiceProperties serviceProperties

    void setup() {
        this.server = Mock(JobKillServiceGrpc.JobKillServiceImplBase)
        this.grpcServerRule.getServiceRegistry().addService(server)
        this.jobId = UUID.randomUUID().toString()
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
        Runnable task
        Trigger trigger
        StreamObserver<JobKillRegistrationResponse> observer

        when:
        service.start(jobId)

        then:
        1 * server.registerForKillNotification(_ as JobKillRegistrationRequest, _ as StreamObserver<JobKillRegistrationResponse>) >> {
            args ->
                (args[0] as JobKillRegistrationRequest).getJobId() == jobId
                observer = args[1] as StreamObserver<JobKillRegistrationResponse>
        }
        1 * taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                task = args[0] as Runnable
                trigger = args[1] as Trigger
                return periodicTaskScheduledFuture
        }
        task != null
        trigger != null
        observer != null

        when:
        task.run()

        then:
        noExceptionThrown()

        when:
        service.stop()

        then:
        1 * periodicTaskScheduledFuture.cancel(true)
    }

    def "Kill"() {
        Runnable task
        ExponentialBackOffTrigger trigger
        StreamObserver<JobKillRegistrationResponse> observer

        when:
        service.start(jobId)

        then:
        1 * server.registerForKillNotification(_ as JobKillRegistrationRequest, _ as StreamObserver<JobKillRegistrationResponse>) >> {
            args ->
                (args[0] as JobKillRegistrationRequest).getJobId() == jobId
                observer = args[1] as StreamObserver<JobKillRegistrationResponse>
        }
        1 * taskScheduler.schedule(_ as Runnable, _ as ExponentialBackOffTrigger) >> {
            args ->
                task = args[0] as Runnable
                trigger = args[1] as ExponentialBackOffTrigger
                return periodicTaskScheduledFuture
        }
        observer != null
        task != null
        trigger != null

        when:
        task.run()

        then:
        0 * server.registerForKillNotification(_, _)

        when:
        observer.onNext(JobKillRegistrationResponse.newInstance())
        observer.onCompleted()

        then:
        noExceptionThrown()

        when:
        task.run()

        then:
        1 * killService.kill(KillService.KillSource.API_KILL_REQUEST)
    }


    def "Error"() {
        Runnable task
        ExponentialBackOffTrigger trigger
        StreamObserver<JobKillRegistrationResponse> observer

        when:
        service.start(jobId)

        then:
        1 * server.registerForKillNotification(_ as JobKillRegistrationRequest, _ as StreamObserver<JobKillRegistrationResponse>) >> {
            args ->
                (args[0] as JobKillRegistrationRequest).getJobId() == jobId
                observer = args[1] as StreamObserver<JobKillRegistrationResponse>
        }
        1 * taskScheduler.schedule(_ as Runnable, _ as ExponentialBackOffTrigger) >> {
            args ->
                task = args[0] as Runnable
                trigger = args[1] as ExponentialBackOffTrigger
                return periodicTaskScheduledFuture
        }
        observer != null
        task != null
        trigger != null

        when:
        observer.onError(new RuntimeException())

        then:
        noExceptionThrown()

        when:
        task.run()

        then:
        0 * killService.kill(KillService.KillSource.API_KILL_REQUEST)

        when:
        task.run()

        then:
        1 * server.registerForKillNotification(_ as JobKillRegistrationRequest, _ as StreamObserver<JobKillRegistrationResponse>) >> {
            args ->
                (args[0] as JobKillRegistrationRequest).getJobId() == jobId
                observer = args[1] as StreamObserver<JobKillRegistrationResponse>
        }
    }
}
