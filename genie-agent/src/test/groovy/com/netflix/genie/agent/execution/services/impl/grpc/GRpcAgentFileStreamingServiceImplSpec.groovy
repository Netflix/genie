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

import com.beust.jcommander.internal.Lists
import com.google.common.collect.Maps
import com.google.protobuf.ByteString
import com.netflix.genie.agent.execution.services.AgentFileStreamingService
import com.netflix.genie.proto.*
import com.netflix.genie.test.categories.UnitTest
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.assertj.core.util.Sets
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import org.springframework.scheduling.TaskScheduler
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.util.concurrent.ScheduledFuture

@Category(UnitTest)
class GRpcAgentFileStreamingServiceImplSpec extends Specification {

    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    @Rule
    TemporaryFolder temporaryFolder

    TestService remoteService
    AgentFileStreamingService service
    JobFileServiceGrpc.JobFileServiceStub client
    TaskScheduler taskScheduler
    String jobId
    List<AgentJobFileMessage> messagesReceived
    Map<StreamObserver<AgentJobFileMessage>, StreamObserver<ServerJobFileMessage>> openStreams
    Map<StreamObserver<AgentJobFileMessage>, StreamObserver<ServerJobFileMessage>> erroredStreams
    Map<StreamObserver<AgentJobFileMessage>, StreamObserver<ServerJobFileMessage>> completedStreams
    ByteString chunk

    ServerJobFileMessage ack = ServerJobFileMessage.newBuilder()
        .setFileStreamAck(
            FileStreamAck.newBuilder()
            .build()
        )
        .build()

    void setup() {
        this.messagesReceived = Lists.newArrayList()
        this.openStreams = Maps.newHashMap()
        this.erroredStreams = Maps.newHashMap()
        this.completedStreams = Maps.newHashMap()
        this.jobId = UUID.randomUUID().toString()
        this.remoteService = new TestService()
        this.grpcServerRule.getServiceRegistry().addService(remoteService)
        this.client = JobFileServiceGrpc.newStub(grpcServerRule.getChannel())
        this.taskScheduler = Mock(TaskScheduler)
        this.service = new GRpcAgentFileStreamingServiceImpl(client, taskScheduler)
    }

    void cleanup() {
        this.grpcServerRule.getChannel().shutdownNow()
    }

    def "Start twice, stop"() {
        when:
        service.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL
        completedStreams.size() + erroredStreams.size() == 0
        messagesReceived.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL
        messagesReceived.get(0).getFileStreamRegistration().getJobId() == jobId
        messagesReceived.get(0) == messagesReceived.get(1)
        messagesReceived.get(1) == messagesReceived.get(2)

        when:
        service.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        thrown(IllegalStateException)

        when:
        service.stop()

        then:
        openStreams.size() == 0
        completedStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL
        messagesReceived.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL
    }

    def "Stream file"() {
        setup:
        File dir = temporaryFolder.newFolder()
        File f = new File(dir, "foo.txt")
        Path relativePath = temporaryFolder.getRoot().toPath().relativize(f.toPath())
        long fileSize = GRpcAgentFileStreamingServiceImpl.MAX_CHUNK_SIZE + (GRpcAgentFileStreamingServiceImpl.MAX_CHUNK_SIZE / 2)
        writeFile(f, fileSize)

        ServerJobFileMessage fileRequest = ServerJobFileMessage.newBuilder()
            .setFileStreamRequest(
                FileStreamRequest.newBuilder()
                .setRelativePath(relativePath.toString())
                .setStartOffset(0)
                .setEndOffset(fileSize)
                .build()
            )
            .build()

        long bytesReceived = 0

        when:
        service.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when:
        Map.Entry<StreamObserver<AgentJobFileMessage>, StreamObserver<ServerJobFileMessage>> streamObservers
        streamObservers = openStreams.entrySet().iterator().next()

        messagesReceived.clear()
        streamObservers.getValue().onNext(fileRequest)

        then:
        messagesReceived.size() >= 1
        messagesReceived.get(0).messageCase == AgentJobFileMessage.MessageCase.FILE_STREAM_CHUNK
        (chunk = messagesReceived.get(0).getFileStreamChunk().getFileChunk()) != null
        (bytesReceived += chunk.size()) == GRpcAgentFileStreamingServiceImpl.MAX_CHUNK_SIZE

        when:
        messagesReceived.clear()
        streamObservers.getValue().onNext(ack)

        then:
        messagesReceived.size() >= 1
        messagesReceived.get(0).messageCase == AgentJobFileMessage.MessageCase.FILE_STREAM_CHUNK
        (chunk = messagesReceived.get(0).getFileStreamChunk().getFileChunk()) != null
        (bytesReceived += chunk.size()) == fileSize

        when:
        messagesReceived.clear()
        streamObservers.getValue().onNext(ack)

        then:
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when:
        service.stop()

        then:
        openStreams.size() == 0
        completedStreams.size() == 4
    }

    def "Misc protocol errors"() {
        setup:
        Path relativePath = Paths.get("foo", "bar.txt")

        ServerJobFileMessage fileRequest = ServerJobFileMessage.newBuilder()
            .setFileStreamRequest(
                FileStreamRequest.newBuilder()
                    .setRelativePath(relativePath.toString())
                    .setStartOffset(0)
                    .setEndOffset(1000)
                    .build()
            )
            .build()

        Map.Entry<StreamObserver<AgentJobFileMessage>, StreamObserver<ServerJobFileMessage>> streamObservers

        when:
        service.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when: "A non-existing file is requested"
        streamObservers = openStreams.entrySet().iterator().next()
        streamObservers.getValue().onNext(fileRequest)

        then: "The corresponding stream is closed by the agent"
        erroredStreams.get(streamObservers.getKey()) == streamObservers.getValue()
        openStreams.size() > 0

        when: "A ack is sent on the wrong stream"
        streamObservers = openStreams.entrySet().iterator().next()
        streamObservers.getValue().onNext(ack)

        then: "The corresponding stream is closed by the agent"
        erroredStreams.get(streamObservers.getKey()) == streamObservers.getValue()
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when:
        service.stop()

        then:
        openStreams.size() == 0
    }

    def "Exhaust available streams"() {
        setup:
        File dir = temporaryFolder.newFolder()
        File f = new File(dir, "foo.txt")
        Path relativePath = temporaryFolder.getRoot().toPath().relativize(f.toPath())
        long fileSize = GRpcAgentFileStreamingServiceImpl.MAX_CHUNK_SIZE / 2
        writeFile(f, fileSize)

        ServerJobFileMessage fileRequest = ServerJobFileMessage.newBuilder()
            .setFileStreamRequest(
            FileStreamRequest.newBuilder()
                .setRelativePath(relativePath.toString())
                .setStartOffset(0)
                .setEndOffset(fileSize)
                .build()
            )
            .build()

        Set<StreamObserver<AgentJobFileMessage>> usedStreams = Sets.newHashSet()

        when:
        service.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when: "Start the maximum number of parallel file transfers"
        for (int i = 0; i < GRpcAgentFileStreamingServiceImpl.MAX_STREAMS; i++) {

            StreamObserver<AgentJobFileMessage> requestObserver
            StreamObserver<ServerJobFileMessage> responseObserver
            for (StreamObserver<AgentJobFileMessage> observer in openStreams.keySet()) {
                if (!usedStreams.contains(observer)) {
                    requestObserver = observer
                    responseObserver = openStreams.get(observer)
                    usedStreams.add(observer)
                    break
                }
            }
            assert requestObserver != null
            assert responseObserver != null
            responseObserver.onNext(fileRequest)
        }

        then: "Chunks are received in each stream"
        messagesReceived.stream()
            .filter({message -> message.getMessageCase() == AgentJobFileMessage.MessageCase.FILE_STREAM_CHUNK})
            .map({message -> message.getFileStreamChunk()})
            .filter({message -> message.getFileChunk().size() == fileSize})
            .collect()
            .size() == GRpcAgentFileStreamingServiceImpl.MAX_STREAMS
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.MAX_STREAMS
        usedStreams.size() == GRpcAgentFileStreamingServiceImpl.MAX_STREAMS

        when: "All streams are ACK'd (and thus completed due to the file size being smaller than a chunk)"
        usedStreams.forEach({observer -> openStreams.get(observer).onNext(ack)})

        then: "Streams are completed and new ones opened"
        completedStreams.size() == GRpcAgentFileStreamingServiceImpl.MAX_STREAMS
        erroredStreams.size() == 0
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when:
        service.stop()

        then:
        openStreams.size() == 0
    }

    def "Handle invalid server requests"() {
        setup:
        File dir = temporaryFolder.newFolder()
        File f = new File(dir, "foo.txt")
        long fileSize = GRpcAgentFileStreamingServiceImpl.MAX_CHUNK_SIZE / 2
        writeFile(f, fileSize)

        Path relativePath = temporaryFolder.getRoot().toPath().relativize(f.toPath())

        ServerJobFileMessage invalidFileRequest = ServerJobFileMessage.newBuilder()
            .setFileStreamRequest(
                FileStreamRequest.newBuilder()
                    .setRelativePath(relativePath.toString())
                    .setStartOffset(fileSize*2)
                    .setEndOffset(fileSize)
                    .build()
            )
            .build()

        ServerJobFileMessage invalidDirRequest = ServerJobFileMessage.newBuilder()
            .setFileStreamRequest(
                FileStreamRequest.newBuilder()
                    .setRelativePath(dir.getName())
                    .setStartOffset(0)
                    .setEndOffset(100)
                    .build()
            )
            .build()

        ServerJobFileMessage fileRequest = ServerJobFileMessage.newBuilder()
            .setFileStreamRequest(
                FileStreamRequest.newBuilder()
                    .setRelativePath(relativePath.toString())
                    .setStartOffset(0)
                    .setEndOffset(fileSize)
                    .build()
            )
            .build()

        Map.Entry<StreamObserver<AgentJobFileMessage>, StreamObserver<ServerJobFileMessage>> stream

        ScheduledFuture delayedRetryTask = Mock(ScheduledFuture)
        Runnable delayedStreamInitRunnable

        when:
        service.start(jobId, temporaryFolder.getRoot().toPath())

        then:
        openStreams.size() > 0

        when: "Request a directory rather than file"
        stream = openStreams.entrySet().iterator().next()
        stream.getValue().onNext(invalidDirRequest)

        then: "The stream gets shut down"
        erroredStreams.size() == 1
        erroredStreams.containsKey(stream.getKey())
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when: "Request a file with invalid offsets"
        stream = openStreams.entrySet().iterator().next()
        stream.getValue().onNext(invalidFileRequest)

        then: "The stream gets shut down"
        erroredStreams.size() == 2
        erroredStreams.containsKey(stream.getKey())
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when: "Send a unsolicited ACK"
        stream = openStreams.entrySet().iterator().next()
        stream.getValue().onNext(ack)

        then: "The stream gets shut down"
        erroredStreams.size() == 3
        erroredStreams.containsKey(stream.getKey())
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when: "Complete a stream"
        stream = openStreams.entrySet().iterator().next()
        stream.getValue().onCompleted()

        then: "The stream gets shut down (without sending back a completion)"
        erroredStreams.size() == 3
        completedStreams.size() == 0
        openStreams.remove(stream.getKey())
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when: "Error on ready stream"
        stream = openStreams.entrySet().iterator().next()
        stream.getValue().onError(new IOException("..."))

        then: "The stream gets shut down (without sending back an error)"
        erroredStreams.size() == 3
        openStreams.remove(stream.getKey())
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when: "Request a valid file, then shut down the stream"
        stream = openStreams.entrySet().iterator().next()
        stream.getValue().onNext(fileRequest)
        stream.getValue().onError(new IOException("..."))

        then: "The stream gets shut down (without sending back an error)"
        erroredStreams.size() == 3
        openStreams.remove(stream.getKey())
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when: "Stream error as it would be thrown if the server was unreachable"
        stream = openStreams.entrySet().iterator().next()
        stream.getValue().onError(new StatusRuntimeException(Status.UNAVAILABLE))

        then: "The stream gets shut down (without sending back an error) and a task is scheduled"
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            args ->
                delayedStreamInitRunnable = args[0] as Runnable
            return delayedRetryTask
        }
        erroredStreams.size() == 3
        openStreams.remove(stream.getKey())
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL - 1
        delayedStreamInitRunnable != null

        when: "Another stream error as it would be thrown if the server was unreachable"
        stream = openStreams.entrySet().iterator().next()
        stream.getValue().onError(new StatusRuntimeException(Status.UNAVAILABLE))

        then: "The stream gets shut down (without sending back an error) and no task is scheduled"
        0 * taskScheduler.schedule(_ as Runnable, _ as Instant)
        erroredStreams.size() == 3
        openStreams.remove(stream.getKey())
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL - 2
        1 * delayedRetryTask.isDone() >> false

        when: "The delayed retry executes"
        delayedStreamInitRunnable.run()

        then: "The number of stream is restored"
        openStreams.size() == GRpcAgentFileStreamingServiceImpl.READY_STREAMS_POOL

        when:
        service.stop()

        then:
        openStreams.size() == 0
    }

    class TestService extends JobFileServiceGrpc.JobFileServiceImplBase {
        @Override
        StreamObserver<AgentJobFileMessage> streamFile(final StreamObserver<ServerJobFileMessage> responseObserver) {
            StreamObserver<AgentJobFileMessage> requestObserver = new StreamObserver<AgentJobFileMessage>() {

                @Override
                void onNext(final AgentJobFileMessage value) {
                    messagesReceived.add(value)
                }

                @Override
                void onError(final Throwable t) {
                    erroredStreams.put(this, openStreams.remove(this))
                }

                @Override
                void onCompleted() {
                    completedStreams.put(this, openStreams.remove(this))
                }
            }

            openStreams.put(requestObserver, responseObserver)
            return requestObserver
        }
    }

    def writeFile(final File file, final long fileSize) {
        byte[] b = new byte[fileSize]
        new Random().nextBytes(b)
        DataOutputStream outputStream = file.newDataOutputStream()
        outputStream.write(b)
        outputStream.flush()
        outputStream.close()
    }

}
