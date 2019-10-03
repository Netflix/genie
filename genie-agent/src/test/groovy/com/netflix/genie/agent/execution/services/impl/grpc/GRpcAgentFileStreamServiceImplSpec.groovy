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

import com.google.common.collect.Maps
import com.netflix.genie.agent.execution.services.AgentFileStreamService
import com.netflix.genie.common.internal.dto.DirectoryManifest
import com.netflix.genie.common.internal.dto.v4.converters.JobDirectoryManifestProtoConverter
import com.netflix.genie.common.internal.exceptions.checked.GenieConversionException
import com.netflix.genie.common.internal.services.JobDirectoryManifestService
import com.netflix.genie.proto.AgentFileMessage
import com.netflix.genie.proto.AgentManifestMessage
import com.netflix.genie.proto.FileStreamServiceGrpc
import com.netflix.genie.proto.ServerAckMessage
import com.netflix.genie.proto.ServerControlMessage
import com.netflix.genie.proto.ServerFileRequestMessage
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.assertj.core.util.Lists
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.Trigger
import spock.lang.Specification

import java.util.concurrent.ScheduledFuture

class GRpcAgentFileStreamServiceImplSpec extends Specification {

    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()

    @Rule
    TemporaryFolder temporaryFolder = new TemporaryFolder()

    AgentFileStreamService agentFileStreamService
    FileStreamServiceGrpc.FileStreamServiceStub client
    TaskScheduler taskScheduler
    JobDirectoryManifestProtoConverter converter
    JobDirectoryManifestService jobDirectoryManifestService
    String jobId
    ScheduledFuture<?> scheduledTask
    RemoteService remoteService
    DirectoryManifest manifest

    void setup() {
        this.jobId = UUID.randomUUID().toString()
        this.manifest = Mock(DirectoryManifest)

        this.scheduledTask = Mock(ScheduledFuture)
        this.taskScheduler = Mock(TaskScheduler)
        this.converter = Mock(JobDirectoryManifestProtoConverter)
        this.jobDirectoryManifestService = Mock(JobDirectoryManifestService)

        this.remoteService = new RemoteService()
        this.grpcServerRule.getServiceRegistry().addService(remoteService)
        this.client = FileStreamServiceGrpc.newStub(grpcServerRule.getChannel())
        this.agentFileStreamService = new GRpcAgentFileStreamServiceImpl(
            client,
            taskScheduler,
            converter,
            jobDirectoryManifestService
        )
    }

    void cleanup() {
        this.grpcServerRule.getChannel().shutdownNow()
    }

    def "Double start, double stop"() {
        setup:
        Runnable runnableCapture

        when:
        agentFileStreamService.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        1 * this.taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                runnableCapture = args[0] as Runnable
                return scheduledTask
        }
        runnableCapture != null

        when:
        agentFileStreamService.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        thrown(IllegalStateException)

        when:
        agentFileStreamService.stop()

        then:
        1 * scheduledTask.cancel(false)

        when:
        agentFileStreamService.stop()

        then:
        noExceptionThrown()
    }

    def "Push manifest and handle errors"() {

        setup:
        Runnable runnableCapture
        AgentManifestMessage manifestMessage = AgentManifestMessage.getDefaultInstance()

        when:
        agentFileStreamService.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        1 * this.taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                runnableCapture = args[0] as Runnable
                return scheduledTask
        }
        runnableCapture != null

        when:
        runnableCapture.run()

        then: "A sync channel is open and a manifest is transmitted"
        1 * jobDirectoryManifestService.getDirectoryManifest(temporaryFolder.getRoot().toPath()) >> manifest
        1 * converter.manifestToProtoMessage(jobId, manifest) >> manifestMessage
        1 == remoteService.activeSyncStreams.size()
        1 == remoteService.manifestMessageReceived.size()
        manifestMessage == remoteService.manifestMessageReceived.get(0)

        when:
        runnableCapture.run()

        then: "Handle manifest creation exception"
        1 * jobDirectoryManifestService.getDirectoryManifest(temporaryFolder.getRoot().toPath()) >> manifest
        1 * converter.manifestToProtoMessage(jobId, manifest) >> { throw new IOException("...") }
        1 == remoteService.activeSyncStreams.size()

        when:
        runnableCapture.run()

        then: "Handle manifest message conversion exception"
        1 * jobDirectoryManifestService.getDirectoryManifest(temporaryFolder.getRoot().toPath()) >> manifest
        1 * converter.manifestToProtoMessage(jobId, manifest) >> {
            throw new GenieConversionException("...")
        }
        1 == remoteService.activeSyncStreams.size()

        when:
        runnableCapture.run()

        then: "Another manifest is transmitted over the existing sync channel"
        1 * jobDirectoryManifestService.getDirectoryManifest(temporaryFolder.getRoot().toPath()) >> manifest
        1 * converter.manifestToProtoMessage(jobId, manifest) >> manifestMessage
        1 == remoteService.activeSyncStreams.size()
        2 == remoteService.manifestMessageReceived.size()
        manifestMessage == remoteService.manifestMessageReceived.get(1)

        when:
        agentFileStreamService.forceServerSync()

        then:
        1 * this.jobDirectoryManifestService.invalidateCachedDirectoryManifest(temporaryFolder.getRoot().toPath())
        1 * jobDirectoryManifestService.getDirectoryManifest(temporaryFolder.getRoot().toPath()) >> manifest
        1 * converter.manifestToProtoMessage(jobId, manifest) >> manifestMessage
        1 == remoteService.activeSyncStreams.size()
        3 == remoteService.manifestMessageReceived.size()
        manifestMessage == remoteService.manifestMessageReceived.get(2)

        when:
        this.grpcServerRule.getChannel().shutdownNow()
        runnableCapture.run()

        then:
        1 * jobDirectoryManifestService.getDirectoryManifest(temporaryFolder.getRoot().toPath()) >> manifest
        1 * converter.manifestToProtoMessage(jobId, manifest) >> manifestMessage

        when:
        agentFileStreamService.stop()

        then:
        1 * scheduledTask.cancel(false)
        0 == remoteService.activeSyncStreams.size()
        0 == remoteService.completedSyncStreams.size()
        1 == remoteService.erroredSyncStreams.size()
    }

    def "Reconnect after stream closed from server"() {

        setup:
        Runnable runnableCapture
        AgentManifestMessage manifestMessage = AgentManifestMessage.getDefaultInstance()

        when:
        agentFileStreamService.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        1 * this.taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                runnableCapture = args[0] as Runnable
                return scheduledTask
        }
        runnableCapture != null

        when:
        runnableCapture.run()

        then: "A sync channel is open and a manifest is transmitted"
        1 * jobDirectoryManifestService.getDirectoryManifest(temporaryFolder.getRoot().toPath()) >> manifest
        1 * converter.manifestToProtoMessage(jobId, manifest) >> manifestMessage
        1 == remoteService.activeSyncStreams.size()
        1 == remoteService.manifestMessageReceived.size()
        manifestMessage == remoteService.manifestMessageReceived.get(0)

        when: "Stream is closed from server"
        remoteService.activeSyncStreams.entrySet().iterator().next().getValue().onError(new RuntimeException("..."))

        then:
        noExceptionThrown()

        when:
        agentFileStreamService.stop()

        then:
        1 * scheduledTask.cancel(false)
        1 == remoteService.activeSyncStreams.size()
        0 == remoteService.completedSyncStreams.size()
        0 == remoteService.erroredSyncStreams.size()
    }

    def "Transmit empty file"() {

        setup:
        temporaryFolder.newFile("file.txt")
        File smallFile = temporaryFolder.newFile("small-file.txt")
        for (int i = 0; i < 10; i++) {
            smallFile.write("Hello world!\n")
        }

        Runnable runnableCapture
        AgentManifestMessage manifestMessage = AgentManifestMessage.getDefaultInstance()

        when:
        agentFileStreamService.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        1 * this.taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                runnableCapture = args[0] as Runnable
                return scheduledTask
        }
        runnableCapture != null

        when:
        runnableCapture.run()

        then: "A sync channel is open and a manifest is transmitted"
        1 * jobDirectoryManifestService.getDirectoryManifest(temporaryFolder.getRoot().toPath()) >> manifest
        1 * converter.manifestToProtoMessage(jobId, manifest) >> manifestMessage
        1 == remoteService.activeSyncStreams.size()
        1 == remoteService.manifestMessageReceived.size()
        manifestMessage == remoteService.manifestMessageReceived.get(0)

        when: "An empty file is requested"
        StreamObserver<ServerControlMessage> observer = remoteService.activeSyncStreams.entrySet().iterator().next().getValue()
        observer.onNext(
            ServerControlMessage.newBuilder()
                .setServerFileRequest(
                    ServerFileRequestMessage.newBuilder()
                        .setRelativePath("file.txt")
                        .setStreamId(UUID.randomUUID().toString())
                        .setStartOffset(0)
                        .setEndOffset(0)
                        .build()
                )
                .build()
        )

        then: "Expect a stream to be opened and immediately completed"
        1 == remoteService.completedTransmitStreams.size()
        0 == remoteService.activeTransmitStreams.size()
        0 == remoteService.fileMessageReceived.size()

        when: "An non-existent file is requested"
        observer.onNext(
            ServerControlMessage.newBuilder()
                .setServerFileRequest(
                    ServerFileRequestMessage.newBuilder()
                        .setRelativePath("no-such-file.txt")
                        .setStreamId(UUID.randomUUID().toString())
                        .setStartOffset(0)
                        .setEndOffset(1000)
                        .build()
                )
                .build()
        )

        then: "Expect the request to be ignored"
        1 == remoteService.completedTransmitStreams.size()
        0 == remoteService.activeTransmitStreams.size()
        0 == remoteService.fileMessageReceived.size()

        when: "An file is requested (such that the range fits into a single chunk)"
        observer.onNext(
            ServerControlMessage.newBuilder()
                .setServerFileRequest(
                    ServerFileRequestMessage.newBuilder()
                        .setRelativePath("small-file.txt")
                        .setStreamId(UUID.randomUUID().toString())
                        .setStartOffset(0)
                        .setEndOffset(10)
                        .build()
                )
                .build()
        )

        then: "Expect a new chunk on a new transmit stream"
        1 == remoteService.completedTransmitStreams.size()
        1 == remoteService.activeTransmitStreams.size()
        1 == remoteService.fileMessageReceived.size()

        when: "Acknowledge the chunk received"
        remoteService.activeTransmitStreams.values().iterator().next().onNext(
            ServerAckMessage.newBuilder().build()
        )

        then: "Expect the transfer to be completed without further messages"
        2 == remoteService.completedTransmitStreams.size()
        0 == remoteService.activeTransmitStreams.size()
        1 == remoteService.fileMessageReceived.size()

        when: "More files are requested than the service is allowed to stream concurrently"
        for (int i = 0; i < GRpcAgentFileStreamServiceImpl.MAX_CONCURRENT_TRANSMIT_STREAMS; i++) {
            observer.onNext(
                ServerControlMessage.newBuilder()
                    .setServerFileRequest(
                        ServerFileRequestMessage.newBuilder()
                            .setRelativePath("small-file.txt")
                            .setStreamId(UUID.randomUUID().toString())
                            .setStartOffset(0)
                            .setEndOffset(10)
                            .build()
                    )
                    .build()
            )
        }

        then: "Expect the transfers to start"
        2 == remoteService.completedTransmitStreams.size()
        5 == remoteService.activeTransmitStreams.size()
        6 == remoteService.fileMessageReceived.size()

        when: "Yet another file is requested"
        observer.onNext(
            ServerControlMessage.newBuilder()
                .setServerFileRequest(
                    ServerFileRequestMessage.newBuilder()
                        .setRelativePath("small-file.txt")
                        .setStreamId(UUID.randomUUID().toString())
                        .setStartOffset(0)
                        .setEndOffset(20)
                        .build()
                )
                .build()
        )

        then: "Expect the request to be ignored"
        2 == remoteService.completedTransmitStreams.size()
        5 == remoteService.activeTransmitStreams.size()
        6 == remoteService.fileMessageReceived.size()

        when: "One transfer is completed"
        remoteService.activeTransmitStreams.values().iterator().next().onNext(
            ServerAckMessage.newBuilder().build()
        )

        then:
        3 == remoteService.completedTransmitStreams.size()
        4 == remoteService.activeTransmitStreams.size()
        6 == remoteService.fileMessageReceived.size()

        when: "Another transfer can be started"
        observer.onNext(
            ServerControlMessage.newBuilder()
                .setServerFileRequest(
                    ServerFileRequestMessage.newBuilder()
                        .setRelativePath("small-file.txt")
                        .setStreamId(UUID.randomUUID().toString())
                        .setStartOffset(0)
                        .setEndOffset(20)
                        .build()
                )
                .build()
        )

        then:
        3 == remoteService.completedTransmitStreams.size()
        5 == remoteService.activeTransmitStreams.size()
        7 == remoteService.fileMessageReceived.size()

        when:
        agentFileStreamService.stop()

        then:
        1 * scheduledTask.cancel(false)
        3 == remoteService.completedTransmitStreams.size()
        5 == remoteService.erroredTransmitStreams.size()
        0 == remoteService.activeTransmitStreams.size()
        1 == remoteService.completedSyncStreams.size()
    }

    class RemoteService extends FileStreamServiceGrpc.FileStreamServiceImplBase {

        Map<StreamObserver<AgentManifestMessage>, StreamObserver<ServerControlMessage>> activeSyncStreams = Maps.newHashMap()
        Map<StreamObserver<AgentManifestMessage>, StreamObserver<ServerControlMessage>> erroredSyncStreams = Maps.newHashMap()
        Map<StreamObserver<AgentManifestMessage>, StreamObserver<ServerControlMessage>> completedSyncStreams = Maps.newHashMap()
        List<AgentManifestMessage> manifestMessageReceived = Lists.newArrayList()

        Map<StreamObserver<AgentFileMessage>, StreamObserver<ServerAckMessage>> activeTransmitStreams = Maps.newHashMap()
        Map<StreamObserver<AgentFileMessage>, StreamObserver<ServerAckMessage>> erroredTransmitStreams = Maps.newHashMap()
        Map<StreamObserver<AgentFileMessage>, StreamObserver<ServerAckMessage>> completedTransmitStreams = Maps.newHashMap()
        List<AgentFileMessage> fileMessageReceived = Lists.newArrayList()

        @Override
        StreamObserver<AgentManifestMessage> sync(final StreamObserver<ServerControlMessage> responseObserver) {

            println("New sync stream")

            StreamObserver<AgentManifestMessage> requestObserver = new StreamObserver<AgentManifestMessage>() {

                @Override
                void onNext(final AgentManifestMessage value) {
                    printf("Received manifest")
                    manifestMessageReceived.add(value)
                }

                @Override
                void onError(final Throwable t) {
                    printf("Sync stream error")
                    StreamObserver<ServerControlMessage> observer = activeSyncStreams.remove(this)
                    erroredSyncStreams.put(this, observer)
                }

                @Override
                void onCompleted() {
                    printf("Sync stream completed")
                    StreamObserver<ServerControlMessage> observer = activeSyncStreams.remove(this)
                    completedSyncStreams.put(this, observer)
                }
            }

            this.activeSyncStreams.put(requestObserver, responseObserver)

            return requestObserver
        }

        @Override
        StreamObserver<AgentFileMessage> transmit(final StreamObserver<ServerAckMessage> responseObserver) {

            println("New transmit stream")

            StreamObserver<AgentFileMessage> requestObserver = new StreamObserver<AgentFileMessage>() {

                @Override
                void onNext(final AgentFileMessage value) {
                    printf("Received file chunk")
                    fileMessageReceived.add(value)
                }

                @Override
                void onError(final Throwable t) {
                    printf("Transmit stream error")
                    StreamObserver<ServerAckMessage> observer = activeTransmitStreams.remove(this)
                    observer.onError(t)
                    erroredTransmitStreams.put(this, observer)
                }

                @Override
                void onCompleted() {
                    printf("Transmit stream completed")
                    StreamObserver<ServerAckMessage> observer = activeTransmitStreams.remove(this)
                    observer.onCompleted()
                    completedTransmitStreams.put(this, observer)
                }
            }

            this.activeTransmitStreams.put(requestObserver, responseObserver)

            return requestObserver
        }
    }
}
