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

import com.google.protobuf.AbstractMessage
import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.States
import com.netflix.genie.agent.execution.statemachine.actions.StateAction
import com.netflix.genie.common.dto.v4.JobSpecification
import com.netflix.genie.proto.*
import com.netflix.genie.test.categories.UnitTest
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.assertj.core.util.Lists
import org.junit.Rule
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest)
class GRpcAgentEventsServiceImplSpec extends Specification {
//
//    @Rule
//    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
//    JobExecutionEventServiceGrpc.JobExecutionEventServiceBlockingStub client
//    Random random
//    float serverErrorProbability = 0
//    GRpcAgentEventsServiceImpl service
//    ExecutionContext executionContext
//    StateChangeEvent stateChangeEvent
//    JobCompletionEvent jobCompletionEvent
//    StateActionCompletionEvent stateActionCompletionEvent
//    String agentId
//    JobSpecification jobSpec
//    JobSpecification.ExecutionResource job
//    String jobId
//    final List<JobExecutionEvent> capturedRequests = Lists.newArrayList()
//
//    void setup() {
//        this.client = JobExecutionEventServiceGrpc.newBlockingStub(grpcServerRule.getChannel())
//        this.random = new Random()
//
//        grpcServerRule.getServiceRegistry().addService(new JobExecutionEventServiceGrpc.JobExecutionEventServiceImplBase() {
//            @Override
//            void publishEvent(
//                    JobExecutionEvent request,
//                    StreamObserver<JobExecutionEventAcknowledgment> responseObserver
//            ) {
//                synchronized (capturedRequests) {
//                    capturedRequests.add(request)
//                    capturedRequests.notify()
//                }
//
//                if (serverErrorProbability > 0 && random.nextFloat() <= serverErrorProbability) {
//                    responseObserver.onError(new RuntimeException("Server error"))
//                } else {
//                    responseObserver.onNext(JobExecutionEventAcknowledgment.newBuilder().build())
//                    responseObserver.onCompleted()
//                }
//            }
//        })
//        this.executionContext = Mock(ExecutionContext)
//        this.service = new GRpcAgentEventsServiceImpl(executionContext, client)
//        this.agentId = "agent-foo-1234"
//        this.jobSpec = Mock(JobSpecification)
//        this.job = Mock(JobSpecification.ExecutionResource)
//        this.jobId = "job-12345"
//
//        this.stateChangeEvent = StateChangeEvent.newBuilder()
//                .setFromState(States.LAUNCH_JOB.name())
//                .setToState(States.MONITOR_JOB.name())
//                .build()
//
//        this.jobCompletionEvent = JobCompletionEvent.newBuilder()
//                .setExecutionDuration(random.nextInt(10000))
//                .addStateActionErrors(
//                StateActionError.newBuilder()
//                        .setState(States.SETUP_JOB.name())
//                        .setAction(StateAction.SetUpJob.getClass().getCanonicalName())
//                        .setExceptionClass(IOException.class.getCanonicalName())
//                        .setExceptionMessage("No such file /foo/bar")
//                        .build()
//        )
//                .build()
//
//        this.stateActionCompletionEvent = StateActionCompletionEvent.newBuilder()
//                .setAction(StateAction.ConfigureAgent.getClass().getCanonicalName())
//                .setDuration(random.nextInt(1000))
//                .setState(States.CONFIGURE_AGENT.name())
//                .build()
//    }
//
//    void cleanup() {
//        service.cleanUp()
//    }
//
//    def "PublishStateChange"() {
//        setup:
//        when:
//        service.publishStateChange(stateChangeEvent)
//
//        then:
//        1 * executionContext.getAgentId() >> agentId
//        1 * executionContext.getJobSpecification() >> jobSpec
//        1 * jobSpec.getJob() >> job
//        1 * job.getId() >> jobId
//        waitForCapturedRequests(1)
//        checkCapturedRequest(capturedRequests.get(0), stateChangeEvent)
//    }
//
//    def "PublishStateChange with nulls"() {
//        setup:
//
//        // Null 'from' state
//        this.stateChangeEvent = StateChangeEvent.newBuilder()
//                .setToState(States.MONITOR_JOB.name())
//                .build()
//        this.agentId = null
//        this.jobSpec = null
//
//        when:
//        service.publishStateChange(stateChangeEvent)
//
//        then:
//        1 * executionContext.getAgentId() >> agentId
//        1 * executionContext.getJobSpecification() >> jobSpec
//        waitForCapturedRequests(1)
//        "" == capturedRequests.get(0).getAgentId()
//        "" == capturedRequests.get(0).getJobId()
//        null != capturedRequests.get(0).getTimestamp()
//        stateChangeEvent == capturedRequests.get(0).getStateChangeEvent()
//
//    }
//
//    def "PublishJobCompletion"() {
//        setup:
//        when:
//        service.publishJobCompletion(jobCompletionEvent)
//
//        then:
//        1 * executionContext.getAgentId() >> agentId
//        1 * executionContext.getJobSpecification() >> jobSpec
//        1 * jobSpec.getJob() >> job
//        1 * job.getId() >> jobId
//        waitForCapturedRequests(1)
//        checkCapturedRequest(capturedRequests.get(0), jobCompletionEvent)
//    }
//
//    def "PublishStateActionCompletion"() {
//        setup:
//        when:
//        service.publishStateActionCompletion(stateActionCompletionEvent)
//
//        then:
//        1 * executionContext.getAgentId() >> agentId
//        1 * executionContext.getJobSpecification() >> jobSpec
//        1 * jobSpec.getJob() >> job
//        1 * job.getId() >> jobId
//        waitForCapturedRequests(1)
//        checkCapturedRequest(capturedRequests.get(0), stateActionCompletionEvent)
//    }
//
//    def "Delivery with server errors"() {
//        setup:
//        serverErrorProbability = 0.2
//        int iterations = 10
//        int totalMessages = 3 * iterations
//
//        when:
//        for (int i = 0; i < iterations; i++) {
//            service.publishJobCompletion(jobCompletionEvent)
//            service.publishStateActionCompletion(stateActionCompletionEvent)
//            service.publishStateChange(stateChangeEvent)
//        }
//
//        then:
//        totalMessages * executionContext.getAgentId() >> agentId
//        totalMessages * executionContext.getJobSpecification() >> jobSpec
//        totalMessages * jobSpec.getJob() >> job
//        totalMessages * job.getId() >> jobId
//        waitForCapturedRequests(totalMessages)
//    }
//
//    void waitForCapturedRequests(int numRequests) {
//        while (true) {
//            synchronized (capturedRequests) {
//                capturedRequests.wait(100)
//                if (capturedRequests.size() >= numRequests) {
//                    break
//                }
//            }
//        }
//    }
//
//    void checkCapturedRequest(JobExecutionEvent jobExecutionEvent, AbstractMessage expectedEventMessage) {
//        assert agentId == jobExecutionEvent.getAgentId()
//        assert jobId == jobExecutionEvent.getJobId()
//        assert null != jobExecutionEvent.getTimestamp()
//        AbstractMessage capturedEventMessage
//        switch (expectedEventMessage.getClass()) {
//            case StateActionCompletionEvent:
//                capturedEventMessage = jobExecutionEvent.getStateActionCompletionEvent()
//                break
//            case JobCompletionEvent:
//                capturedEventMessage = jobExecutionEvent.getJobCompletionEvent()
//                break
//            case StateChangeEvent:
//                capturedEventMessage = jobExecutionEvent.getStateChangeEvent()
//                break
//            default:
//                throw new RuntimeException("Unknown event class: " + expectedEventMessage.getClass().getSimpleName())
//        }
//        assert expectedEventMessage == capturedEventMessage
//    }

}
