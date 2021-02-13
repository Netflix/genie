/*
 *
 *  Copyright 2020 Netflix, Inc.
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
import com.netflix.genie.common.internal.dtos.DirectoryManifest
import com.netflix.genie.common.internal.dtos.v4.converters.JobDirectoryManifestProtoConverter
import com.netflix.genie.common.internal.exceptions.checked.GenieConversionException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import com.netflix.genie.proto.AgentFileMessage
import com.netflix.genie.proto.AgentManifestMessage
import com.netflix.genie.proto.ServerAckMessage
import com.netflix.genie.proto.ServerControlMessage
import com.netflix.genie.proto.ServerFileRequestMessage
import com.netflix.genie.web.properties.AgentFileStreamProperties
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import org.junit.platform.commons.util.StringUtils
import org.springframework.core.io.Resource
import org.springframework.http.HttpRange
import org.springframework.scheduling.TaskScheduler
import org.springframework.util.unit.DataSize
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeoutException

class GRpcAgentFileStreamServiceImplSpec extends Specification {
    static final int FILE_SIZE = 100

    GRpcAgentFileStreamServiceImpl service
    JobDirectoryManifestProtoConverter converter
    TaskScheduler taskScheduler
    AgentFileStreamProperties serviceProperties
    MeterRegistry registry
    String jobId
    DirectoryManifest directoryManifest
    Path relativePath = Paths.get("stdout")
    URI uri = URI.create("agent://host/stdout")
    DirectoryManifest.ManifestEntry manifestEntry
    AgentManifestMessage manifestMessage
    StreamObserver<ServerControlMessage> controlStreamResponseObserver
    StreamObserver<ServerAckMessage> transferStreamResponseObserver
    Runnable stalledTransfersTask

    void setup() {
        this.converter = Mock(JobDirectoryManifestProtoConverter)
        this.taskScheduler = Mock(TaskScheduler) {
            scheduleAtFixedRate(_ as Runnable, _ as Duration) >> {
                Runnable r, Duration d ->
                    this.stalledTransfersTask = r
                    return null
            }
        }
        this.serviceProperties = Mock(AgentFileStreamProperties) {
            getUnclaimedStreamStartTimeout() >> Duration.ofSeconds(3)
            getMaxConcurrentTransfers() >> 10
            getStalledTransferCheckInterval() >> Duration.ofSeconds(3)
            getWriteRetryDelay() >> Duration.ofMillis(250)
            getStalledTransferTimeout() >> Duration.ofSeconds(5)
            getManifestCacheExpiration() >> Duration.ofSeconds(10)
        }
        this.registry = Mock(MeterRegistry) {
            counter(_ as String) >> Mock(Counter)
            summary(_ as String) >> Mock(DistributionSummary)
        }
        this.jobId = UUID.randomUUID().toString()
        this.controlStreamResponseObserver = Mock(StreamObserver)
        this.transferStreamResponseObserver = Mock(StreamObserver)
        this.directoryManifest = Mock(DirectoryManifest)
        this.manifestMessage = AgentManifestMessage.newBuilder().setJobId(jobId).setLargeFilesSupported(true).build()
        this.manifestEntry = Mock(DirectoryManifest.ManifestEntry) {
            getLastModifiedTime() >> Instant.now()
            getSize() >> FILE_SIZE
        }
        this.service = new GRpcAgentFileStreamServiceImpl(converter, taskScheduler, serviceProperties, registry)
        assert this.stalledTransfersTask != null
    }

    def "Get manifest"() {
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver
        AgentManifestMessage manifestMessage = AgentManifestMessage.newBuilder().setJobId(jobId).build()

        Optional<DirectoryManifest> optionalManifest
        when:
        optionalManifest = this.service.getManifest(jobId)

        then:
        !optionalManifest.isPresent()

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)
        optionalManifest = this.service.getManifest(jobId)

        then:
        !optionalManifest.isPresent()

        when: "Manifest sent, fails conversion"
        controlStreamRequestObserver.onNext(manifestMessage)
        optionalManifest = this.service.getManifest(jobId)

        then:
        1 * converter.toManifest(manifestMessage) >> { throw new GenieConversionException("...") }
        !optionalManifest.isPresent()

        when: "Manifest sent successfully"
        controlStreamRequestObserver.onNext(manifestMessage)
        optionalManifest = this.service.getManifest(jobId)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest
        optionalManifest.isPresent()
        optionalManifest.get() == directoryManifest

        when: "Control stream is closed by client"
        controlStreamRequestObserver.onCompleted()
        optionalManifest = this.service.getManifest(jobId)

        then:
        optionalManifest.isPresent()
    }

    def "No manifest"() {
        when: "Request file transfer"
        Optional<Resource> resource = service.getResource(jobId, relativePath, uri, null)

        then:
        !resource.isPresent()
    }

    def "No manifest entry"() {
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)
        controlStreamRequestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest

        when: "Request file transfer"
        Optional<Resource> resource = service.getResource(jobId, relativePath, uri, null)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.empty()
        resource.isPresent()
        !resource.get().exists()
    }

    @Unroll
    def "No control stream due to #description"() {
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)
        controlStreamRequestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest

        when: "Manifest stream is shut down"
        if (error) {
            controlStreamRequestObserver.onError(new GenieRuntimeException("..."))
        } else {
            controlStreamRequestObserver.onCompleted()
        }

        then:
        (error ? 0 : 1) * controlStreamResponseObserver.onCompleted()

        when: "Request file transfer"
        Optional<Resource> resource = service.getResource(jobId, relativePath, uri, null)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(manifestEntry)
        !resource.isPresent()

        where:
        description         | error
        "stream error"      | true
        "stream completion" | false
    }

    @Unroll
    def "Anonymous control stream termination due to #description"() {
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)

        then:
        noExceptionThrown()

        when: "Manifest stream termination"
        if (error) {
            controlStreamRequestObserver.onError(new GenieRuntimeException("..."))
        } else {
            controlStreamRequestObserver.onCompleted()
        }

        then:
        (error ? 0 : 1) * controlStreamResponseObserver.onCompleted()

        where:
        description         | error
        "stream error"      | true
        "stream completion" | false
    }

    def "Newer control stream replaces and closes a previous one"() {
        StreamObserver<ServerControlMessage> controlStreamResponseObserver1 = Mock(StreamObserver)
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver1
        StreamObserver<ServerControlMessage> controlStreamResponseObserver2 = Mock(StreamObserver)
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver2
        AgentManifestMessage manifestMessage = AgentManifestMessage.newBuilder().setJobId(jobId).build()


        when: "Control stream established"
        controlStreamRequestObserver1 = this.service.sync(controlStreamResponseObserver1)
        controlStreamRequestObserver1.onNext(manifestMessage)
        controlStreamRequestObserver1.onNext(manifestMessage)
        controlStreamRequestObserver1.onNext(manifestMessage)

        then:
        3 * converter.toManifest(manifestMessage) >> directoryManifest

        when: "Another Control stream established for the same job"
        controlStreamRequestObserver2 = this.service.sync(controlStreamResponseObserver2)
        controlStreamRequestObserver2.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest
        1 * controlStreamResponseObserver1.onError(_ as IllegalStateException)
    }

    def "Handle control stream error"() {
        StreamObserver<ServerControlMessage> controlStreamResponseObserver1 = Mock(StreamObserver)
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver1
        StreamObserver<ServerControlMessage> controlStreamResponseObserver2 = Mock(StreamObserver)
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver2
        AgentManifestMessage manifestMessage = AgentManifestMessage.newBuilder().setJobId(jobId).build()

        when: "Control stream established and error before receiving manifest"
        controlStreamRequestObserver1 = this.service.sync(controlStreamResponseObserver1)
        controlStreamRequestObserver1.onError(new RuntimeException("..."))

        then:
        0 * controlStreamResponseObserver1.onCompleted()

        when: "Another Control stream established and error after sending a manifest"
        controlStreamRequestObserver2 = this.service.sync(controlStreamResponseObserver2)
        controlStreamRequestObserver2.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest
        0 * controlStreamResponseObserver2._
    }

    @Unroll
    def "Successful transfer with range: #range (legacy agent: #legacy)"() {
        this.manifestMessage = AgentManifestMessage.newBuilder().setJobId(jobId).setLargeFilesSupported(!legacy).build()
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver
        StreamObserver<AgentFileMessage> transferStreamRequestObserver
        String streamId
        InputStream inputStream
        int bytesRead
        Runnable streamTimeoutTask

        ServerFileRequestMessage fileRequestCapture

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)
        controlStreamRequestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest

        when: "Request file transfer"
        Optional<Resource> resource = service.getResource(jobId, relativePath, uri, range)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(manifestEntry)
        1 * controlStreamResponseObserver.onNext(_ as ServerControlMessage) >> {
            ServerControlMessage msg ->
                fileRequestCapture = msg.getServerFileRequest()
                streamId = fileRequestCapture.getStreamId()
        }
        resource.isPresent()
        fileRequestCapture != null
        fileRequestCapture.getDeprecatedStartOffset() == offsetStart
        fileRequestCapture.getDeprecatedEndOffset() == offsetEnd
        fileRequestCapture.getStartOffset() == offsetStart
        fileRequestCapture.getEndOffset() == offsetEnd
        streamId != null
        StringUtils.isNotBlank(streamId)
        fileRequestCapture.getRelativePath() == relativePath.toString()

        when: "File transfer begins"
        transferStreamRequestObserver = this.service.transmit(transferStreamResponseObserver)
        transferStreamRequestObserver.onNext(
            AgentFileMessage.newBuilder()
                .setStreamId(streamId)
                .setData(ByteString.copyFrom(new byte[chunkSize]))
                .build()
        )

        then:
        transferStreamRequestObserver != null
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            Runnable r, Instant i ->
                streamTimeoutTask = r
                return null
        }
        1 * taskScheduler.schedule(_ as Runnable, _ as Date) >> {
            runnable, date ->
                runnable.run()
        }
        1 * transferStreamResponseObserver.onNext(_ as ServerAckMessage)
        streamTimeoutTask != null

        when: "Both timeout tasks run"
        streamTimeoutTask.run()
        stalledTransfersTask.run()

        then:
        noExceptionThrown()

        when: "Read data"
        inputStream = resource.get().getInputStream()
        inputStream.skip(offsetStart)
        bytesRead = inputStream.read(new byte[512])

        then:
        bytesRead == chunkSize

        when: "Read more"
        transferStreamRequestObserver.onCompleted()
        bytesRead = inputStream.read(new byte[512])

        then:
        1 * transferStreamResponseObserver.onCompleted()
        bytesRead == -1

        where:
        range                              | offsetStart | offsetEnd | chunkSize | legacy
        null                               | 0           | FILE_SIZE | FILE_SIZE | true
        HttpRange.createByteRange(50)      | 50          | FILE_SIZE | 50        | true
        HttpRange.createSuffixRange(50)    | 50          | FILE_SIZE | 50        | true
        HttpRange.createByteRange(10, 20)  | 10          | 21        | 10        | true
        HttpRange.createByteRange(50, 300) | 50          | FILE_SIZE | 50        | true
        null                               | 0           | FILE_SIZE | FILE_SIZE | false
        HttpRange.createByteRange(50)      | 50          | FILE_SIZE | 50        | false
        HttpRange.createSuffixRange(50)    | 50          | FILE_SIZE | 50        | false
        HttpRange.createByteRange(10, 20)  | 10          | 21        | 10        | false
        HttpRange.createByteRange(50, 300) | 50          | FILE_SIZE | 50        | false
    }

    def "Request large file from legacy agent"() {
        this.manifestMessage = AgentManifestMessage.newBuilder().setJobId(jobId).setLargeFilesSupported(false).build()
        int chunkSize = 512
        DataSize largeFileSize = DataSize.ofGigabytes(10)
        DataSize suffixSize = DataSize.ofBytes(chunkSize * 3)
        HttpRange range = HttpRange.createSuffixRange(suffixSize.toBytes())
        DirectoryManifest.ManifestEntry largeFileManifestEntry = Mock(DirectoryManifest.ManifestEntry) {
            getLastModifiedTime() >> Instant.now()
            getSize() >> largeFileSize.toBytes()
        }
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)
        controlStreamRequestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest

        when: "Request file transfer"
        Optional<Resource> resource = service.getResource(jobId, relativePath, uri, range)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(largeFileManifestEntry)
        0 * controlStreamResponseObserver.onNext(_ as ServerControlMessage)
        !resource.isPresent()
    }

    def "Request tail of very large file"() {
        int chunkSize = 512
        DataSize largeFileSize = DataSize.ofGigabytes(10)
        DataSize suffixSize = DataSize.ofBytes(chunkSize * 3)
        HttpRange range = HttpRange.createSuffixRange(suffixSize.toBytes())
        DirectoryManifest.ManifestEntry largeFileManifestEntry = Mock(DirectoryManifest.ManifestEntry) {
            getLastModifiedTime() >> Instant.now()
            getSize() >> largeFileSize.toBytes()
        }

        StreamObserver<AgentManifestMessage> controlStreamRequestObserver
        StreamObserver<AgentFileMessage> transferStreamRequestObserver
        String streamId
        InputStream inputStream
        int bytesRead

        ServerFileRequestMessage fileRequestCapture

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)
        controlStreamRequestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest

        when: "Request file transfer"
        Optional<Resource> resource = service.getResource(jobId, relativePath, uri, range)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(largeFileManifestEntry)
        1 * controlStreamResponseObserver.onNext(_ as ServerControlMessage) >> {
            ServerControlMessage msg ->
                fileRequestCapture = msg.getServerFileRequest()
                streamId = fileRequestCapture.getStreamId()
        }
        resource.isPresent()
        fileRequestCapture != null
        fileRequestCapture.getStartOffset() == largeFileSize.toBytes() - suffixSize.toBytes()
        fileRequestCapture.getEndOffset() == largeFileSize.toBytes()
        streamId != null
        StringUtils.isNotBlank(streamId)
        fileRequestCapture.getRelativePath() == relativePath.toString()

        when: "File transfer begins, send chunk 1/3"
        transferStreamRequestObserver = this.service.transmit(transferStreamResponseObserver)
        transferStreamRequestObserver.onNext(
            AgentFileMessage.newBuilder()
                .setStreamId(streamId)
                .setData(ByteString.copyFrom(new byte[chunkSize]))
                .build()
        )

        then:
        transferStreamRequestObserver != null
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant)
        1 * taskScheduler.schedule(_ as Runnable, _ as Date) >> {
            runnable, date ->
                runnable.run()
        }
        1 * transferStreamResponseObserver.onNext(_ as ServerAckMessage)

        when: "Read data (chunk 1/3)"
        inputStream = resource.get().getInputStream()
        inputStream.skip(largeFileSize.toBytes() - suffixSize.toBytes())
        bytesRead = inputStream.read(new byte[512])

        then:
        bytesRead == chunkSize

        when: "send chunk 2/3"
        transferStreamRequestObserver.onNext(
            AgentFileMessage.newBuilder()
                .setStreamId(streamId)
                .setData(ByteString.copyFrom(new byte[chunkSize]))
                .build()
        )

        then:
        transferStreamRequestObserver != null
        1 * taskScheduler.schedule(_ as Runnable, _ as Date) >> {
            runnable, date ->
                runnable.run()
        }
        1 * transferStreamResponseObserver.onNext(_ as ServerAckMessage)

        when: "Read data (chunk 2/3)"
        bytesRead = inputStream.read(new byte[512])

        then:
        bytesRead == chunkSize

        when: "send chunk 3/3"
        transferStreamRequestObserver.onNext(
            AgentFileMessage.newBuilder()
                .setStreamId(streamId)
                .setData(ByteString.copyFrom(new byte[chunkSize]))
                .build()
        )

        then:
        transferStreamRequestObserver != null
        1 * taskScheduler.schedule(_ as Runnable, _ as Date) >> {
            runnable, date ->
                runnable.run()
        }
        1 * transferStreamResponseObserver.onNext(_ as ServerAckMessage)

        when: "Read data (chunk 3/3)"
        bytesRead = inputStream.read(new byte[512])

        then:
        bytesRead == chunkSize

        when: "Complete reading"
        transferStreamRequestObserver.onCompleted()
        bytesRead = inputStream.read(new byte[512])

        then:
        1 * transferStreamResponseObserver.onCompleted()
        bytesRead == -1
    }

    @Unroll
    def "Request empty file or empty range (range: #range size: #fileSize)"() {
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver
        InputStream inputStream
        int bytesRead

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)
        controlStreamRequestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest

        when: "Request file transfer"
        Optional<Resource> resource = service.getResource(jobId, relativePath, uri, range)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(manifestEntry)
        2 * manifestEntry.getSize() >> fileSize
        0 * controlStreamResponseObserver.onNext(_ as ServerControlMessage)
        0 * taskScheduler.schedule(_ as Runnable, _ as Instant)
        resource.isPresent()

        when: "Read data"
        inputStream = resource.get().getInputStream()
        inputStream.skip(skip)
        bytesRead = inputStream.read(new byte[512])

        then:
        bytesRead == -1

        where:
        range                               | fileSize | skip
        null                                | 0        | 0
        HttpRange.createByteRange(100)      | 100      | 100
        HttpRange.createByteRange(100, 200) | 100      | 100
    }

    def "Transfer start timeout"() {
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver
        StreamObserver<AgentFileMessage> transferStreamRequestObserver
        Optional<Resource> resource
        Runnable streamTimeoutTask

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)
        controlStreamRequestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest

        when: "Request file transfer"
        resource = service.getResource(jobId, relativePath, uri, null)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(manifestEntry)
        1 * controlStreamResponseObserver.onNext(_ as ServerControlMessage)
        resource.isPresent()

        when: "Timeout transfer before transfer stream is established"
        stalledTransfersTask.run()

        then:
        1 * serviceProperties.getStalledTransferTimeout() >> Duration.ofSeconds(-1)

        when: "Read data"
        resource.get().getInputStream().read(new byte[512])

        then:
        thrown(IOException)

        when: "Request file transfer"
        resource = service.getResource(jobId, relativePath, uri, null)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(manifestEntry)
        1 * controlStreamResponseObserver.onNext(_ as ServerControlMessage)
        resource.isPresent()

        when: "Transfer stream is established but not associated with the transfer"
        transferStreamRequestObserver = this.service.transmit(transferStreamResponseObserver)

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            Runnable r, Instant i ->
                streamTimeoutTask = r
                return null
        }
        transferStreamRequestObserver != null
        resource.isPresent()
        streamTimeoutTask != null

        when: "Timeout transfer before the stream sends its first message"
        stalledTransfersTask.run()

        then:
        1 * serviceProperties.getStalledTransferTimeout() >> Duration.ofSeconds(-1)
        0 * transferStreamResponseObserver.onError(_ as TimeoutException)

        when:
        streamTimeoutTask.run()

        then:
        1 * transferStreamResponseObserver.onError(_ as TimeoutException)

        when: "Read data"
        resource.get().getInputStream().read(new byte[512])

        then:
        thrown(IOException)
    }

    def "Unclaimed stream timeout"() {
        StreamObserver<AgentFileMessage> transferStreamRequestObserver

        Runnable streamTimeoutTask

        when: "File transfer stream established"
        transferStreamRequestObserver = this.service.transmit(transferStreamResponseObserver)

        then:
        transferStreamRequestObserver != null
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            Runnable r, Instant i ->
                streamTimeoutTask = r
                return null
        }
        streamTimeoutTask != null

        when: "New stream has not been claimed"
        streamTimeoutTask.run()

        then:
        1 * transferStreamResponseObserver.onError(_ as TimeoutException)
    }

    def "Unclaimed stream error"() {
        StreamObserver<AgentFileMessage> transferStreamRequestObserver

        Runnable streamTimeoutTask

        when: "File transfer stream established"
        transferStreamRequestObserver = this.service.transmit(transferStreamResponseObserver)

        then:
        transferStreamRequestObserver != null
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            Runnable r, Instant i ->
                streamTimeoutTask = r
                return null
        }
        streamTimeoutTask != null

        when: "Unclaimed stream error"
        transferStreamRequestObserver.onError(new RuntimeException("..."))

        then:
        0 * transferStreamResponseObserver.onCompleted()

        when: "Unclaimed stream timeout after error"
        streamTimeoutTask.run()

        then:
        0 * transferStreamResponseObserver._
    }

    def "In progress stream error"() {
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver
        StreamObserver<AgentFileMessage> transferStreamRequestObserver
        String streamId
        InputStream inputStream
        int bytesRead

        ServerFileRequestMessage fileRequestCapture

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)
        controlStreamRequestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest

        when: "Request file transfer"
        Optional<Resource> resource = service.getResource(jobId, relativePath, uri, null)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(manifestEntry)
        1 * controlStreamResponseObserver.onNext(_ as ServerControlMessage) >> {
            ServerControlMessage msg ->
                fileRequestCapture = msg.getServerFileRequest()
                streamId = fileRequestCapture.getStreamId()
        }
        resource.isPresent()
        fileRequestCapture != null
        streamId != null
        StringUtils.isNotBlank(streamId)

        when: "File transfer begins"
        transferStreamRequestObserver = this.service.transmit(transferStreamResponseObserver)
        transferStreamRequestObserver.onNext(
            AgentFileMessage.newBuilder()
                .setStreamId(streamId)
                .setData(ByteString.copyFrom(new byte[FILE_SIZE / 2]))
                .build()
        )

        then:
        transferStreamRequestObserver != null
        1 * taskScheduler.schedule(_ as Runnable, _ as Date) >> {
            runnable, date -> runnable.run() // Write data in buffer and send ack
        }
        1 * transferStreamResponseObserver.onNext(_ as ServerAckMessage)

        when: "Transfer stream error"
        transferStreamRequestObserver.onError(new RuntimeException("..."))

        then:
        0 * transferStreamResponseObserver.onCompleted()

        when: "Read data"
        inputStream = resource.get().getInputStream()
        bytesRead = inputStream.read(new byte[512])

        then:
        bytesRead == FILE_SIZE / 2 as int

        when: "Read more"
        inputStream.read(new byte[512])

        then:
        thrown(IOException)
    }

    def "Exceed maximum number of transfers"() {
        StreamObserver<AgentManifestMessage> controlStreamRequestObserver

        when: "Control stream established"
        controlStreamRequestObserver = this.service.sync(controlStreamResponseObserver)
        controlStreamRequestObserver.onNext(manifestMessage)

        then:
        1 * converter.toManifest(manifestMessage) >> directoryManifest

        when: "Request 2 file transfers"
        Optional<Resource> resource1 = service.getResource(jobId, relativePath, uri, null)
        Optional<Resource> resource2 = service.getResource(jobId, relativePath, uri, null)

        then:
        2 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(manifestEntry)
        2 * serviceProperties.getMaxConcurrentTransfers() >> 2
        2 * controlStreamResponseObserver.onNext(_ as ServerControlMessage)
        resource1.isPresent()
        resource2.isPresent()

        when: "Request one more file transfer"
        Optional<Resource> resource3 = service.getResource(jobId, relativePath, uri, null)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(manifestEntry)
        1 * serviceProperties.getMaxConcurrentTransfers() >> 2
        0 * controlStreamResponseObserver.onNext(_ as ServerControlMessage)
        !resource3.isPresent()

        when: "One of the transfers times out"
        stalledTransfersTask.run()

        then:
        2 * serviceProperties.getStalledTransferTimeout() >> Duration.ofSeconds(-1) >> Duration.ofSeconds(100)

        when: "Retry to request file transfer"
        resource3 = service.getResource(jobId, relativePath, uri, null)

        then:
        1 * directoryManifest.getEntry(relativePath.toString()) >> Optional.of(manifestEntry)
        1 * serviceProperties.getMaxConcurrentTransfers() >> 2
        1 * controlStreamResponseObserver.onNext(_ as ServerControlMessage)
        resource3.isPresent()
    }
}
