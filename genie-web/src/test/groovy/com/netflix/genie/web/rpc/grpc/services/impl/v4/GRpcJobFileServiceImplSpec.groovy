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

package com.netflix.genie.web.rpc.grpc.services.impl.v4

import com.google.protobuf.ByteString
import com.google.protobuf.Empty
import com.netflix.genie.common.internal.dto.JobDirectoryManifest
import com.netflix.genie.common.internal.dto.v4.converters.JobDirectoryManifestProtoConverter
import com.netflix.genie.common.internal.exceptions.GenieConversionException
import com.netflix.genie.common.internal.exceptions.StreamUnavailableException
import com.netflix.genie.common.internal.util.FileBuffer
import com.netflix.genie.common.internal.util.FileBufferFactory
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.proto.*
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.services.AgentFileManifestService
import com.netflix.genie.web.services.AgentFileStreamService
import io.grpc.stub.StreamObserver
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths

@Category(UnitTest)
class GRpcJobFileServiceImplSpec extends Specification {

    static final String JOB_ID = "12345"

    @Rule
    TemporaryFolder temporaryFolder

    JobDirectoryManifestProtoConverter converter
    AgentFileManifestService manifestService
    AgentFileStreamService fileStreamingService
    FileBufferFactory fileBufferFactory
    StreamObserver<Empty> manifestResponseObserver
    StreamObserver<ServerJobFileMessage> streamResponseObserver
    Path relativePath
    FileBuffer fileBuffer

    GRpcJobFileServiceImpl service

    JobDirectoryManifest manifest
    JobFileManifestMessage manifestMessage
    AgentJobFileMessage registrationMessage
    AgentJobFileMessage chunkMessage
    ByteString chunkBytes
    OutputStream outputStream

    void setup() {
        this.converter = Mock(JobDirectoryManifestProtoConverter)
        this.manifestService = Mock(AgentFileManifestService)
        this.fileStreamingService = Mock(AgentFileStreamService)
        this.fileBufferFactory = Mock(FileBufferFactory)
        this.manifestResponseObserver = Mock(StreamObserver)
        this.streamResponseObserver = Mock(StreamObserver)
        this.relativePath = Paths.get("foo/")
        this.fileBuffer = Mock(FileBuffer)

        byte[] bytes = new byte[100]
        Arrays.fill(bytes, 64 as byte)
        this.chunkBytes = ByteString.copyFrom(bytes)

        this.outputStream = Mock(OutputStream)

        this.temporaryFolder.newFile("file1.txt")
        this.temporaryFolder.newFolder("directory").createNewFile()

        this.manifest = new JobDirectoryManifest(temporaryFolder.getRoot().toPath(), false)
        this.manifestMessage = new JobDirectoryManifestProtoConverter(GenieObjectMapper.getMapper()).manifestToProtoMessage(JOB_ID, manifest)

        this.registrationMessage = AgentJobFileMessage.newBuilder()
            .setFileStreamRegistration(
                FileStreamRegistration.newBuilder()
                    .setJobId(JOB_ID)
                    .build()
            )
            .build()

        this.chunkMessage = AgentJobFileMessage.newBuilder()
            .setFileStreamChunk(
                FileStreamChunk.newBuilder()
                    .setFileChunk(chunkBytes)
                    .build()
            )
            .build()

        this.service = new GRpcJobFileServiceImpl(
            this.converter,
            this.manifestService,
            this.fileStreamingService,
            this.fileBufferFactory
        )
    }

    def "PushManifest"() {
        setup:
        StreamObserver<JobFileManifestMessage> requestObserver

        when:
        requestObserver = service.pushManifest(manifestResponseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> manifest
        1 * manifestService.updateManifest(JOB_ID, manifest)

        when:
        requestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> manifest
        1 * manifestService.updateManifest(JOB_ID, manifest)

        when:
        requestObserver.onCompleted()

        then:
        1 * manifestService.deleteManifest(JOB_ID)
        1 * manifestResponseObserver.onCompleted()
    }

    def "PushManifest - error before first manifest"() {
        setup:
        StreamObserver<JobFileManifestMessage> requestObserver

        when:
        requestObserver = service.pushManifest(manifestResponseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onError(new IOException("..."))

        then:
        1 * manifestResponseObserver.onCompleted()
        0 * manifestService.deleteManifest(JOB_ID)
    }

    def "PushManifest - completion before first manifest"() {
        setup:
        StreamObserver<JobFileManifestMessage> requestObserver

        when:
        requestObserver = service.pushManifest(manifestResponseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onCompleted()

        then:
        1 * manifestResponseObserver.onCompleted()
        0 * manifestService.deleteManifest(JOB_ID)
    }

    def "PushManifest - manifest conversion error"() {
        setup:
        StreamObserver<JobFileManifestMessage> requestObserver

        when:
        requestObserver = service.pushManifest(manifestResponseObserver)

        then:
        requestObserver != null

        when:
        requestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> manifest
        1 * manifestService.updateManifest(JOB_ID, manifest)

        when:
        requestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> { throw new GenieConversionException("...") }
        0 * manifestService.updateManifest(JOB_ID, manifest)

        when:
        requestObserver.onError(new IOException("..."))

        then:
        1 * manifestService.deleteManifest(JOB_ID)
        1 * manifestResponseObserver.onCompleted()
    }


    def "StreamFile - uninitialized error"() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onError()

        then:
        1 * streamResponseObserver.onCompleted()
    }

    def "StreamFile - uninitialized completion"() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onCompleted()

        then:
        1 * streamResponseObserver.onCompleted()
    }

    def "StreamFile - uninitialized bad message"() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onNext(AgentJobFileMessage.getDefaultInstance())

        then:
        1 * streamResponseObserver.onCompleted()
    }

    def "StreamFile - ready error"() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver
        AgentFileStreamService.ReadyStream readyStreamCapture

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onNext(registrationMessage)

        then:
        1 * fileStreamingService.registerReadyStream(_ as AgentFileStreamService.ReadyStream) >> {
            args ->
                readyStreamCapture = args[0] as AgentFileStreamService.ReadyStream
        }
        readyStreamCapture != null
        readyStreamCapture.getJobId() == JOB_ID

        when:
        requestObserver.onError(new IOException("..."))

        then:
        1 * fileStreamingService.unregisterReadyStream(readyStreamCapture)
        1 * streamResponseObserver.onCompleted()
    }

    def "StreamFile - ready completed (then attempt activation)"() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver
        AgentFileStreamService.ReadyStream readyStreamCapture

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onNext(registrationMessage)

        then:
        1 * fileStreamingService.registerReadyStream(_ as AgentFileStreamService.ReadyStream) >> {
            args ->
                readyStreamCapture = args[0] as AgentFileStreamService.ReadyStream
        }
        readyStreamCapture != null
        readyStreamCapture.getJobId() == JOB_ID

        when:
        requestObserver.onCompleted()

        then:
        1 * fileStreamingService.unregisterReadyStream(readyStreamCapture)
        1 * streamResponseObserver.onCompleted()
    }

    def "StreamFile - ready bad message (then attempt activation)"() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver
        AgentFileStreamService.ReadyStream readyStreamCapture

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onNext(registrationMessage)

        then:
        1 * fileStreamingService.registerReadyStream(_ as AgentFileStreamService.ReadyStream) >> {
            args ->
                readyStreamCapture = args[0] as AgentFileStreamService.ReadyStream
        }
        readyStreamCapture != null
        readyStreamCapture.getJobId() == JOB_ID

        when:
        requestObserver.onNext(AgentJobFileMessage.getDefaultInstance())

        then:
        1 * fileStreamingService.unregisterReadyStream(readyStreamCapture)
        1 * streamResponseObserver.onCompleted()

        when:
        readyStreamCapture.activateStream(relativePath, 0, 1000)

        then:
        thrown(StreamUnavailableException)
    }

    def "StreamFile - active successful transfer"() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver
        AgentFileStreamService.ReadyStream readyStreamCapture
        AgentFileStreamService.ActiveStream activeStream
        ServerJobFileMessage serverJobFileMessageCapture

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onNext(registrationMessage)

        then:
        1 * fileStreamingService.registerReadyStream(_ as AgentFileStreamService.ReadyStream) >> {
            args ->
                readyStreamCapture = args[0] as AgentFileStreamService.ReadyStream
        }
        readyStreamCapture != null
        readyStreamCapture.getJobId() == JOB_ID

        when:
        activeStream = readyStreamCapture.activateStream(relativePath, 0, 200)

        then:
        1 * fileBufferFactory.get(200) >> fileBuffer
        1 * fileBuffer.getOutputStream() >> outputStream
        activeStream.getJobId() == JOB_ID
        activeStream.getRelativePath() == relativePath
        1 * streamResponseObserver.onNext(_ as ServerJobFileMessage) >> {
            args ->
                serverJobFileMessageCapture = args[0] as ServerJobFileMessage
        }
        serverJobFileMessageCapture != null
        serverJobFileMessageCapture.getMessageCase() == ServerJobFileMessage.MessageCase.FILE_STREAM_REQUEST
        serverJobFileMessageCapture.getFileStreamRequest().getRelativePath() == "foo"
        serverJobFileMessageCapture.getFileStreamRequest().getStartOffset() == 0
        serverJobFileMessageCapture.getFileStreamRequest().getEndOffset() == 200

        when:
        requestObserver.onNext(chunkMessage)

        then:
        1 * outputStream.write(chunkMessage.getFileStreamChunk().getFileChunk().toByteArray())
        1 * outputStream.flush()
        1 * streamResponseObserver.onNext(_ as ServerJobFileMessage) >> {
            args ->
                serverJobFileMessageCapture = args[0] as ServerJobFileMessage
        }
        serverJobFileMessageCapture != null
        serverJobFileMessageCapture.getMessageCase() == ServerJobFileMessage.MessageCase.FILE_STREAM_ACK

        when:
        requestObserver.onNext(chunkMessage)

        then:
        1 * outputStream.write(chunkMessage.getFileStreamChunk().getFileChunk().toByteArray())
        1 * outputStream.flush()
        1 * streamResponseObserver.onNext(_ as ServerJobFileMessage) >> {
            args ->
                serverJobFileMessageCapture = args[0] as ServerJobFileMessage
        }
        serverJobFileMessageCapture != null
        serverJobFileMessageCapture.getMessageCase() == ServerJobFileMessage.MessageCase.FILE_STREAM_ACK

        when:
        requestObserver.onCompleted()

        then:
        1 * streamResponseObserver.onCompleted()
        1 * outputStream.close()
    }

    def "StreamFile - active incomplete transfer "() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver
        AgentFileStreamService.ReadyStream readyStreamCapture
        AgentFileStreamService.ActiveStream activeStream
        ServerJobFileMessage serverJobFileMessageCapture

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onNext(registrationMessage)

        then:
        1 * fileStreamingService.registerReadyStream(_ as AgentFileStreamService.ReadyStream) >> {
            args ->
                readyStreamCapture = args[0] as AgentFileStreamService.ReadyStream
        }
        readyStreamCapture != null
        readyStreamCapture.getJobId() == JOB_ID

        when:
        activeStream = readyStreamCapture.activateStream(relativePath, 0, 200)

        then:
        1 * fileBufferFactory.get(200) >> fileBuffer
        1 * fileBuffer.getOutputStream() >> outputStream
        activeStream.getJobId() == JOB_ID
        activeStream.getRelativePath() == relativePath
        1 * streamResponseObserver.onNext(_ as ServerJobFileMessage) >> {
            args ->
                serverJobFileMessageCapture = args[0] as ServerJobFileMessage
        }
        serverJobFileMessageCapture != null
        serverJobFileMessageCapture.getMessageCase() == ServerJobFileMessage.MessageCase.FILE_STREAM_REQUEST
        serverJobFileMessageCapture.getFileStreamRequest().getRelativePath() == "foo"
        serverJobFileMessageCapture.getFileStreamRequest().getStartOffset() == 0
        serverJobFileMessageCapture.getFileStreamRequest().getEndOffset() == 200

        when:
        requestObserver.onCompleted()

        then:
        1 * streamResponseObserver.onCompleted()
        1 * outputStream.close()
    }


    def "StreamFile - active write error "() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver
        AgentFileStreamService.ReadyStream readyStreamCapture
        AgentFileStreamService.ActiveStream activeStream
        ServerJobFileMessage serverJobFileMessageCapture

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onNext(registrationMessage)

        then:
        1 * fileStreamingService.registerReadyStream(_ as AgentFileStreamService.ReadyStream) >> {
            args ->
                readyStreamCapture = args[0] as AgentFileStreamService.ReadyStream
        }
        readyStreamCapture != null
        readyStreamCapture.getJobId() == JOB_ID

        when:
        activeStream = readyStreamCapture.activateStream(relativePath, 0, 200)

        then:
        1 * fileBufferFactory.get(200) >> fileBuffer
        1 * fileBuffer.getOutputStream() >> outputStream
        activeStream.getJobId() == JOB_ID
        activeStream.getRelativePath() == relativePath
        1 * streamResponseObserver.onNext(_ as ServerJobFileMessage) >> {
            args ->
                serverJobFileMessageCapture = args[0] as ServerJobFileMessage
        }
        serverJobFileMessageCapture != null
        serverJobFileMessageCapture.getMessageCase() == ServerJobFileMessage.MessageCase.FILE_STREAM_REQUEST
        serverJobFileMessageCapture.getFileStreamRequest().getRelativePath() == "foo"
        serverJobFileMessageCapture.getFileStreamRequest().getStartOffset() == 0
        serverJobFileMessageCapture.getFileStreamRequest().getEndOffset() == 200

        when:
        requestObserver.onNext(chunkMessage)

        then:
        1 * outputStream.write(chunkMessage.getFileStreamChunk().getFileChunk().toByteArray()) >> { throw new IOException("...") }

        then:
        1 * streamResponseObserver.onCompleted()
        1 * outputStream.close()
    }

    def "StreamFile - active error"() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver
        AgentFileStreamService.ReadyStream readyStreamCapture
        AgentFileStreamService.ActiveStream activeStream
        ServerJobFileMessage serverJobFileMessageCapture

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onNext(registrationMessage)

        then:
        1 * fileStreamingService.registerReadyStream(_ as AgentFileStreamService.ReadyStream) >> {
            args ->
                readyStreamCapture = args[0] as AgentFileStreamService.ReadyStream
        }
        readyStreamCapture != null
        readyStreamCapture.getJobId() == JOB_ID

        when:
        activeStream = readyStreamCapture.activateStream(relativePath, 0, 1000)

        then:
        1 * fileBufferFactory.get(1000) >> fileBuffer
        1 * fileBuffer.getOutputStream() >> outputStream
        activeStream.getJobId() == JOB_ID
        activeStream.getRelativePath() == relativePath
        1 * streamResponseObserver.onNext(_ as ServerJobFileMessage) >> {
            args ->
                serverJobFileMessageCapture = args[0] as ServerJobFileMessage
        }
        serverJobFileMessageCapture != null
        serverJobFileMessageCapture.getMessageCase() == ServerJobFileMessage.MessageCase.FILE_STREAM_REQUEST
        serverJobFileMessageCapture.getFileStreamRequest().getRelativePath() == "foo"
        serverJobFileMessageCapture.getFileStreamRequest().getStartOffset() == 0
        serverJobFileMessageCapture.getFileStreamRequest().getEndOffset() == 1000

        when:
        requestObserver.onError(new IOException("..."))

        then:
        1 * streamResponseObserver.onCompleted()
        1 * outputStream.close() >> {throw new IOException("...")}
        noExceptionThrown()
    }


    def "StreamFile - active wrong message type"() {
        setup:
        StreamObserver<AgentJobFileMessage> requestObserver
        AgentFileStreamService.ReadyStream readyStreamCapture
        AgentFileStreamService.ActiveStream activeStream
        ServerJobFileMessage serverJobFileMessageCapture

        when:
        requestObserver = service.streamFile(streamResponseObserver)

        then:
        requestObserver.onNext(registrationMessage)

        then:
        1 * fileStreamingService.registerReadyStream(_ as AgentFileStreamService.ReadyStream) >> {
            args ->
                readyStreamCapture = args[0] as AgentFileStreamService.ReadyStream
        }
        readyStreamCapture != null
        readyStreamCapture.getJobId() == JOB_ID

        when:
        activeStream = readyStreamCapture.activateStream(relativePath, 0, 1000)

        then:
        1 * fileBufferFactory.get(1000) >> fileBuffer
        1 * fileBuffer.getOutputStream() >> outputStream
        activeStream.getJobId() == JOB_ID
        activeStream.getRelativePath() == relativePath
        1 * streamResponseObserver.onNext(_ as ServerJobFileMessage) >> {
            args ->
                serverJobFileMessageCapture = args[0] as ServerJobFileMessage
        }
        serverJobFileMessageCapture != null
        serverJobFileMessageCapture.getMessageCase() == ServerJobFileMessage.MessageCase.FILE_STREAM_REQUEST
        serverJobFileMessageCapture.getFileStreamRequest().getRelativePath() == "foo"
        serverJobFileMessageCapture.getFileStreamRequest().getStartOffset() == 0
        serverJobFileMessageCapture.getFileStreamRequest().getEndOffset() == 1000

        when:
        requestObserver.onNext(AgentJobFileMessage.getDefaultInstance())

        then:
        1 * streamResponseObserver.onCompleted()
        1 * outputStream.close()
    }
}
