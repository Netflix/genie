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

package com.netflix.genie.agent.execution.services.impl.grpc

import com.google.common.collect.Lists
import com.google.protobuf.Empty
import com.netflix.genie.common.internal.dto.JobDirectoryManifest
import com.netflix.genie.common.internal.dto.v4.converters.JobDirectoryManifestProtoConverter
import com.netflix.genie.common.internal.exceptions.GenieConversionException
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.proto.JobFileManifestMessage
import com.netflix.genie.proto.JobFileServiceGrpc
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.Trigger
import spock.lang.Specification

import java.util.concurrent.ScheduledFuture

class GRpcAgentFileManifestServiceImplSpec extends Specification {
    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    @Rule
    TemporaryFolder temporaryFolder

    String jobId
    JobFileServiceGrpc.JobFileServiceStub client
    TaskScheduler taskScheduler
    ScheduledFuture heartBeatFuture
    GRpcAgentFileManifestServiceImpl service
    JobDirectoryManifestProtoConverter converter
    ScheduledFuture scheduledTask
    JobDirectoryManifest manifest
    JobFileManifestMessage manifestMessage
    TestService remoteService

    StreamObserver<Empty> currentResponseObserver
    StreamObserver<JobFileManifestMessage> currentRequestObserver
    List<JobFileManifestMessage> messagesReceived

    void setup() {
        this.currentRequestObserver = null
        this.currentResponseObserver = null

        this.remoteService = new TestService()
        this.grpcServerRule.getServiceRegistry().addService(remoteService)
        this.client = JobFileServiceGrpc.newStub(grpcServerRule.getChannel())
        this.messagesReceived = Lists.newArrayList()

        this.jobId = UUID.randomUUID().toString()
        this.taskScheduler = Mock(TaskScheduler)
        this.heartBeatFuture = Mock(ScheduledFuture)
        this.converter = Mock(JobDirectoryManifestProtoConverter)
        this.scheduledTask = Mock(ScheduledFuture)
        this.manifest = new JobDirectoryManifest(temporaryFolder.getRoot().toPath(), false)
        this.manifestMessage = new JobDirectoryManifestProtoConverter(GenieObjectMapper.getMapper()).manifestToProtoMessage(jobId, manifest)
        this.service = new GRpcAgentFileManifestServiceImpl(client, taskScheduler, converter)
    }

    void cleanup() {
        this.grpcServerRule.getChannel().shutdownNow()
    }

    def "Start twice, stop"() {
        when:
        service.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Trigger) >> scheduledTask

        when:
        service.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        thrown(IllegalStateException)

        when:
        service.stop()

        then:
        1 * scheduledTask.cancel(false)
    }

    def "PushManifest, swallow errors"() {
        setup:
        Runnable runnableCapture
        Trigger triggerCapture

        when:
        service.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                runnableCapture = args[0] as Runnable
                triggerCapture = args[1] as Trigger
                return scheduledTask
        }
        runnableCapture != null
        triggerCapture != null

        when:
        runnableCapture.run()

        then:
        1 * converter.manifestToProtoMessage(jobId, _ as JobDirectoryManifest) >> manifestMessage
        messagesReceived.size() == 1
        messagesReceived[0] == manifestMessage

        when:
        runnableCapture.run()

        then:
        1 * converter.manifestToProtoMessage(jobId, _ as JobDirectoryManifest) >> { throw new IOException() }
        messagesReceived.size() == 1


        when:
        runnableCapture.run()

        then:
        1 * converter.manifestToProtoMessage(jobId, _ as JobDirectoryManifest) >> { throw new GenieConversionException("...") }
        messagesReceived.size() == 1

        when:
        runnableCapture.run()

        then:
        1 * converter.manifestToProtoMessage(jobId, _ as JobDirectoryManifest) >> manifestMessage
        messagesReceived.size() == 2
        messagesReceived[1] == manifestMessage

        when:
        service.stop()

        then:
        1 * scheduledTask.cancel(false)
    }


    def "PushManifest, reset stream on error"() {
        setup:
        Runnable runnableCapture
        Trigger triggerCapture

        when:
        service.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                runnableCapture = args[0] as Runnable
                triggerCapture = args[1] as Trigger
                return scheduledTask
        }
        runnableCapture != null
        triggerCapture != null

        when:
        runnableCapture.run()

        then:
        1 * converter.manifestToProtoMessage(jobId, _ as JobDirectoryManifest) >> manifestMessage
        messagesReceived.size() == 1
        messagesReceived[0] == manifestMessage

        when:
        currentResponseObserver.onError(new IOException("..."))
        runnableCapture.run()

        then:
        1 * converter.manifestToProtoMessage(jobId, _ as JobDirectoryManifest) >> manifestMessage
        messagesReceived.size() == 2
        messagesReceived[1] == manifestMessage

        when:
        currentResponseObserver.onCompleted()
        runnableCapture.run()

        then:
        1 * converter.manifestToProtoMessage(jobId, _ as JobDirectoryManifest) >> manifestMessage
        messagesReceived.size() == 3
        messagesReceived[2] == manifestMessage

        when:
        service.stop()

        then:
        1 * scheduledTask.cancel(false)
    }

    class TestService extends JobFileServiceGrpc.JobFileServiceImplBase {

        @Override
        StreamObserver<JobFileManifestMessage> pushManifest(final StreamObserver<Empty> responseObserver) {
            currentResponseObserver = responseObserver
            currentRequestObserver = new StreamObserver<JobFileManifestMessage>() {

                @Override
                void onNext(final JobFileManifestMessage value) {
                    messagesReceived.add(value)
                }

                @Override
                void onError(final Throwable t) {
                }

                @Override
                void onCompleted() {
                }
            }
            return currentRequestObserver
        }
    }
}
