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
package com.netflix.genie.web.agent.apis.rpc.v4.endpoints

import com.google.protobuf.ByteString
import com.netflix.genie.common.exceptions.GenieTimeoutException
import com.netflix.genie.common.internal.dtos.DirectoryManifest
import com.netflix.genie.common.internal.dtos.v4.converters.JobDirectoryManifestProtoConverter
import com.netflix.genie.common.internal.exceptions.checked.GenieConversionException
import com.netflix.genie.proto.AgentFileMessage
import com.netflix.genie.proto.AgentManifestMessage
import com.netflix.genie.proto.ServerAckMessage
import com.netflix.genie.proto.ServerControlMessage
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.springframework.core.io.Resource
import org.springframework.http.HttpRange
import org.springframework.scheduling.TaskScheduler
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.ByteBuffer
import java.nio.file.Paths
import java.time.Instant

class GRpcAgentFileStreamServiceImplSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder = new TemporaryFolder()

    JobDirectoryManifestProtoConverter converter
    TaskScheduler taskScheduler
    GRpcAgentFileStreamServiceImpl service
    StreamObserver<ServerControlMessage> serverControlObserver
    StreamObserver<ServerAckMessage> serverTransmitObserver
    AgentManifestMessage manifestMessage
    String jobId
    DirectoryManifest manifest
    ByteString data
    @Shared int dataSize
    @Shared long lastOffset
    HttpRange noRange = null

    void setup() {
        this.data = ByteString.copyFromUtf8("Hello World!\n")
        this.temporaryFolder.newFile("foo1.txt")
        this.temporaryFolder.newFile("foo2.txt").write(data.toStringUtf8())
        this.temporaryFolder.newFolder("bar")
        this.dataSize = data.size()
        this.lastOffset = dataSize - 1


        this.converter = Mock(JobDirectoryManifestProtoConverter)
        this.taskScheduler = Mock(TaskScheduler)
        this.service = new GRpcAgentFileStreamServiceImpl(
            converter,
            taskScheduler,
            new SimpleMeterRegistry()
        )
        this.serverControlObserver = Mock(StreamObserver)
        this.serverTransmitObserver = Mock(StreamObserver)

        this.jobId = UUID.randomUUID().toString()
        this.manifestMessage = AgentManifestMessage.newBuilder().setJobId(jobId).build()
        this.manifest = Spy(new DirectoryManifest.Factory().getDirectoryManifest(this.temporaryFolder.getRoot().toPath(), true))
    }

    void cleanup() {
    }

    def "Control stream"() {
        setup:
        Optional<DirectoryManifest> m
        StreamObserver<AgentManifestMessage> o

        when: "Fetch manifest before any agent connected"
        m = service.getManifest(jobId)

        then:
        !m.isPresent()

        when: "Have one agent connect"
        o = service.sync(serverControlObserver)

        then:
        noExceptionThrown()

        when: "Fetch manifest before any manifest is received"
        m = service.getManifest(jobId)

        then:
        !m.isPresent()

        when: "Send a manifest"
        o.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> manifest

        when: "Fetch manifest"
        m = service.getManifest(jobId)

        then:
        m.isPresent()
        m.get() == manifest

        when: "Send a manifest that fails to deserialize"
        o.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> { throw new GenieConversionException("...") }

        when: "Fetch latest valid manifest"
        m = service.getManifest(jobId)

        then:
        m.isPresent()
        m.get() == manifest

        when: "Complete stream"
        o.onCompleted()

        then:
        noExceptionThrown()

        when: "Fetch manifest after agent has disconnected"
        m = service.getManifest(jobId)

        then:
        !m.isPresent()

        when: "Have one agent connect then close the stream with an error"
        service.sync(serverControlObserver).onError(new RuntimeException("..."))

        then:
        noExceptionThrown()
    }

    def "Transfer stream"() {
        StreamObserver<AgentManifestMessage> o
        Optional<Resource> r
        ServerControlMessage c
        Runnable t
        StreamObserver<AgentFileMessage> s

        when: "Agent not connected"
        r = service.getResource(jobId, Paths.get("foo.txt"), null, noRange)

        then:
        !r.isPresent()

        when: "Agent connected and registered, but no manifest"
        o = service.sync(serverControlObserver)
        o.onNext(manifestMessage)
        r = service.getResource(jobId, Paths.get("foo.txt"), null, noRange)

        then:
        1 * converter.toManifest(manifestMessage) >> { throw new GenieConversionException("...") }
        !r.isPresent()

        when: "Send valid manifest"
        o.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> manifest

        when: "Request a file that does not exist"
        r = service.getResource(jobId, Paths.get("does-not-exist.txt"), null, noRange)

        then:
        1 * manifest.getEntry("does-not-exist.txt")
        r.isPresent()
        !r.get().exists()

        when: "Request a file that is empty"
        r = service.getResource(jobId, Paths.get("foo1.txt"), null, noRange)

        then:
        1 * manifest.getEntry("foo1.txt")
        0 * serverControlObserver.onNext(_)
        0 * taskScheduler.schedule(_, _)
        r.isPresent()
        r.get().exists()

        when: "Request a file"
        r = service.getResource(jobId, Paths.get("foo2.txt"), null, noRange)

        then:
        1 * manifest.getEntry("foo2.txt")
        1 * serverControlObserver.onNext(_ as ServerControlMessage) >> { args -> c = args[0] as ServerControlMessage }
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> { args -> t = args[0] as Runnable; return null }
        c != null
        c.getMessageCase() == ServerControlMessage.MessageCase.SERVER_FILE_REQUEST
        c.getServerFileRequest().getStreamId() != ""
        c.getServerFileRequest().getStartOffset() == 0
        c.getServerFileRequest().getEndOffset() == data.size()
        c.getServerFileRequest().getRelativePath() == "foo2.txt"
        t != null
        r.isPresent()
        r.get().exists()

        when: "Timeout before stream is initiated"
        t.run()

        then:
        noExceptionThrown()

        when: "Request a file"
        r = service.getResource(jobId, Paths.get("foo2.txt"), null, noRange)

        then:
        1 * manifest.getEntry("foo2.txt")
        1 * serverControlObserver.onNext(_ as ServerControlMessage) >> { args -> c = args[0] as ServerControlMessage }
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> { args -> t = args[0] as Runnable; return null }
        c != null
        c.getMessageCase() == ServerControlMessage.MessageCase.SERVER_FILE_REQUEST
        c.getServerFileRequest().getStreamId() != ""
        c.getServerFileRequest().getStartOffset() == 0
        c.getServerFileRequest().getEndOffset() == data.size()
        c.getServerFileRequest().getRelativePath() == "foo2.txt"
        t != null
        r.isPresent()
        r.get().exists()

        when: "Agent initiates stream"
        s = service.transmit(serverTransmitObserver)

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> { args -> t = args[0] as Runnable; return null }

        when: "Timeout before first chunk is received"
        t.run()

        then:
        1 * serverTransmitObserver.onError(_ as GenieTimeoutException)

        when: "Request a file"
        r = service.getResource(jobId, Paths.get("foo2.txt"), null, noRange)

        then:
        1 * manifest.getEntry("foo2.txt")
        1 * serverControlObserver.onNext(_ as ServerControlMessage) >> { args -> c = args[0] as ServerControlMessage }
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> { args -> t = args[0] as Runnable; return null }
        c != null
        t != null
        r.isPresent()
        r.get().exists()

        when: "Agent initiates stream"
        s = service.transmit(serverTransmitObserver)

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> { args -> t = args[0] as Runnable; return null }

        when: "First (and only) chunk is sent"
        s.onNext(
            AgentFileMessage.newBuilder()
                .setStreamId(c.getServerFileRequest().getStreamId())
                .setData(data)
                .build()
        )

        then:
        ByteBuffer buffer = ByteBuffer.allocate(data.size())
        r.get().readableChannel().read(buffer)
        buffer.rewind()
        buffer.array() == data.toByteArray()

        when: "Transfer completed"
        s.onCompleted()

        then:
        noExceptionThrown()
    }

    def "Transfer stream errors"() {
        StreamObserver<AgentManifestMessage> o
        Optional<Resource> r
        ServerControlMessage c
        Runnable t
        StreamObserver<AgentFileMessage> s

        o = service.sync(serverControlObserver)

        when: "Send valid manifest"
        o.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> manifest

        when: "Request a file"
        r = service.getResource(jobId, Paths.get("foo2.txt"), null, noRange)

        then:
        1 * manifest.getEntry("foo2.txt")
        1 * serverControlObserver.onNext(_ as ServerControlMessage) >> { args -> c = args[0] as ServerControlMessage }
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> { args -> t = args[0] as Runnable; return null }
        c != null
        t != null
        r.isPresent()
        r.get().exists()

        when: "Agent initiates stream"
        s = service.transmit(serverTransmitObserver)

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> { args -> t = args[0] as Runnable; return null }

        when: "Stream error before first chunk"
        s.onError(new IOException("..."))

        then:
        noExceptionThrown()

        when: "Agent initiates stream"
        s = service.transmit(serverTransmitObserver)

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> { args -> t = args[0] as Runnable; return null }

        when: "First chunk is sent"
        s.onNext(
            AgentFileMessage.newBuilder()
                .setStreamId(c.getServerFileRequest().getStreamId())
                .setData(data)
                .build()
        )

        then:
        noExceptionThrown()

        when: "Stream error during in-progress transfer"
        s.onError(new IOException("..."))

        then:
        noExceptionThrown()

        when: "Agent initiates stream"
        s = service.transmit(serverTransmitObserver)

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> { args -> t = args[0] as Runnable; return null }

        when: "First chunk is sent but stream id does not match the request"
        s.onNext(
            AgentFileMessage.newBuilder()
                .setStreamId("?")
                .setData(data)
                .build()
        )

        then:
        noExceptionThrown()

        when: "First chunk is sent with blank stream id"
        s.onNext(
            AgentFileMessage.newBuilder()
                .setData(data)
                .build()
        )

        then:
        noExceptionThrown()
    }

    @Unroll
    def "Transfer stream with range: #range"() {
        StreamObserver<AgentManifestMessage> o
        Optional<Resource> r
        ServerControlMessage c
        Runnable t
        StreamObserver<AgentFileMessage> s

        when: "Agent connected and registered, but no manifest"
        o = service.sync(serverControlObserver)
        o.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> manifest

        when: "Request a file"
        r = service.getResource(jobId, Paths.get("foo2.txt"), null, range)

        then:
        1 * manifest.getEntry("foo2.txt")
        1 * serverControlObserver.onNext(_ as ServerControlMessage) >> { args -> c = args[0] as ServerControlMessage }
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> { args -> t = args[0] as Runnable; return null }
        c != null
        t != null
        r.isPresent()
        r.get().exists()
        c.getServerFileRequest().getStartOffset() == expectedRangeStart
        c.getServerFileRequest().getEndOffset() == expectedRangeEnd

        where:
        range                             | expectedRangeStart | expectedRangeEnd
        null                              | 0                  | dataSize // No range
        HttpRange.createSuffixRange(5)    | dataSize - 5       | dataSize // Last 5 bytes
        HttpRange.createByteRange(5)      | 5                  | dataSize // All minus first 5 bytes
        HttpRange.createByteRange(4, 8)   | 4                  | 9        // Proper range
        HttpRange.createSuffixRange(120)  | 0                  | dataSize // More data than it's available
        HttpRange.createByteRange(3, 120) | 3                  | dataSize // More data than it's available
        HttpRange.createByteRange(lastOffset, lastOffset)   | lastOffset                  | dataSize        // Last byte
    }

    @Unroll
    def "Transfer stream with empty range: #range for #filename "() {
        StreamObserver<AgentManifestMessage> o
        Optional<Resource> r
        HttpRange

        when: "Agent connected and registered, but no manifest"
        o = service.sync(serverControlObserver)
        o.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> manifest

        when: "Request a file"
        r = service.getResource(jobId, Paths.get(filename), null, range)

        then:
        1 * manifest.getEntry(filename)
        0 * serverControlObserver.onNext(_)
        0 * taskScheduler.schedule(_, _)
        r.isPresent()
        r.get().exists()
        r.get().getInputStream().available() == 0

        where:
        range                                 | filename
        HttpRange.createByteRange(dataSize)   | "foo2.txt"
        HttpRange.createByteRange(1000, 1100) | "foo2.txt"
        HttpRange.createSuffixRange(100)      | "foo1.txt"
    }
}
