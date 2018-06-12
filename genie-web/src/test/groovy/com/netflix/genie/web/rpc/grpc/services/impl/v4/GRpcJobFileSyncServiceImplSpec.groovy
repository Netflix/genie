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
package com.netflix.genie.web.rpc.grpc.services.impl.v4

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.google.protobuf.ByteString
import com.netflix.genie.common.internal.dto.v4.files.JobFileState
import com.netflix.genie.proto.BeginSync
import com.netflix.genie.proto.DataUpload
import com.netflix.genie.proto.DeleteFile
import com.netflix.genie.proto.JobDirectoryState
import com.netflix.genie.proto.JobFileSyncServiceGrpc
import com.netflix.genie.proto.SyncComplete
import com.netflix.genie.proto.SyncRequest
import com.netflix.genie.proto.SyncResponse
import com.netflix.genie.test.suppliers.RandomSuppliers
import com.netflix.genie.web.properties.JobFileSyncRpcProperties
import com.netflix.genie.web.services.JobFileService
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.junit.Rule
import org.springframework.scheduling.TaskScheduler
import spock.lang.Shared
import spock.lang.Specification

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.util.concurrent.ScheduledFuture

/**
 * Specifications for the {@link GRpcJobFileSyncServiceImpl} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
class GRpcJobFileSyncServiceImplSpec extends Specification {

    @Shared
    Charset UTF_8 = StandardCharsets.UTF_8

    @Rule
    GrpcServerRule gRpcServerRule = new GrpcServerRule().directExecutor()

    def cleanup() {
        // Force fast shutdown as the rule would await termination for 2 minutes!
        this.gRpcServerRule.getChannel().shutdownNow()
        this.gRpcServerRule.getServer().shutdownNow()
    }

    /**
     * The logic of the class being tested is highly stateful so in order to have
     * certain things happen others need to happen before (effectively a state machine).
     * As such this is one giant test at the moment.
     *
     * Note: For now this is one giant when/then method. We could break it up using
     * {@link spock.lang.Stepwise} but not sure how this would behave with {@link GrpcServerRule}.
     */
    def "Test job file sync workflow"() {
        def jobFileSyncRpcProperties = new JobFileSyncRpcProperties()
        jobFileSyncRpcProperties.setMaxSyncMessages(3)
        JobFileService jobFileService = Mock()
        TaskScheduler taskScheduler = Mock()
        StreamObserver<SyncResponse> responseObserver = Mock()
        ScheduledFuture<?> scheduledFuture = Mock()
        SyncResponse response
        SyncRequest request
        DataUpload dataUpload
        DeleteFile deleteFile

        def successfulMessageIds = Lists.newArrayList()
        def unsuccessfulMessageIds = Lists.newArrayList()

        GRpcJobFileSyncServiceImpl service
        JobFileSyncServiceGrpc.JobFileSyncServiceStub stub

        def jobId = UUID.randomUUID().toString()
        def beginSync = BeginSync
                .newBuilder()
                .setJobId(jobId)
                .setAcknowledgedAgentDirectoryState(JobDirectoryState.newBuilder().build())
                .build()
        def beginRequest = SyncRequest.newBuilder().setBeginSync(beginSync).build()
        StreamObserver<SyncRequest> requestObserver

        when: "The service is created"
        service = new GRpcJobFileSyncServiceImpl(
                jobFileSyncRpcProperties,
                jobFileService,
                taskScheduler
        )
        this.gRpcServerRule.getServiceRegistry().addService(service)
        stub = JobFileSyncServiceGrpc.newStub(this.gRpcServerRule.getChannel())
        requestObserver = stub.sync(responseObserver)

        then: "A thread is scheduled to send acknowledgements"
        1 * taskScheduler.scheduleWithFixedDelay(
                _ as Runnable,
                jobFileSyncRpcProperties.getAckIntervalMilliseconds()
        ) >> scheduledFuture

        when: "Data is attempted to be uploaded before a begin sync is received"
        requestObserver.onNext(createDataUpload())

        then: "The upload message is ignored and a sync reset message is sent"
        0 * jobFileService.updateFile(_ as String, _ as String, _ as Long, _ as byte[])
        1 * responseObserver.onNext(_ as SyncResponse) >> { arguments -> response = (SyncResponse) arguments[0] }
        response != null
        response.hasReset()
        response.getReset() != null

        when: "A sync complete message is sent before a begin sync is received"
        requestObserver.onNext(createSyncComplete())

        then: "The sync message is ignored and no reset message is sent"
        0 * jobFileService.getJobDirectoryFileState(_ as String, _ as Boolean)
        0 * responseObserver.onNext(_ as SyncResponse)

        when: "A delete file message is sent before a begin sync is received"
        requestObserver.onNext(createDeleteFile())

        then: "The delete message is ignored and no reset message is sent"
        0 * jobFileService.deleteJobFile(_ as String, _ as String)
        0 * responseObserver.onNext(_ as SyncResponse)

        when: "Data upload is attempted after a reset has been sent but before a begin sync message is received"
        requestObserver.onNext(createDataUpload())

        then: "The upload message is ignored and another reset message IS NOT sent"
        0 * jobFileService.updateFile(_ as String, _ as String, _ as Long, _ as byte[])
        0 * responseObserver.onNext(_ as SyncResponse)

        when: "A first begin sync message is sent"
        requestObserver.onNext(beginRequest)

        then:
        "It is acknowledged along with the servers job directory state. " +
                "Also the observer is registered with the sync service singleton"
        1 * jobFileService.getJobDirectoryFileState(jobId, false) >>
                Sets.newHashSet(new JobFileState(UUID.randomUUID().toString(), 100L, null))
        1 * responseObserver.onNext(_ as SyncResponse) >> { arguments -> response = (SyncResponse) arguments[0] }
        response != null
        response.hasBeginAck()
        !response.getBeginAck().getServerDirectoryState().getIncludesMd5()
        response.getBeginAck().getServerDirectoryState().getFilesCount() == 1
        service.jobSyncRequestObservers.size() == 1
        service.jobSyncRequestObservers.containsKey(jobId)

        when: "A begin sync is sent after one has already been received"
        requestObserver.onNext(beginRequest)

        then: "It is ignored and dropped"
        0 * jobFileService.getJobDirectoryFileState(jobId, false)
        0 * responseObserver.onNext(_ as SyncResponse)

        when: "Data is uploaded after a begin sync"
        request = createDataUpload()
        dataUpload = request.getDataUpload()
        successfulMessageIds.add(dataUpload.getId())
        requestObserver.onNext(request)

        then: "It is received but not yet acknowledged"
        1 * jobFileService.updateFile(_ as String, _ as String, _ as Long, _ as byte[]) >> { arguments ->
            assert arguments[0] == jobId
            assert arguments[1] == dataUpload.getPath()
            assert arguments[2] == dataUpload.getStartByte()
            assert arguments[3] == dataUpload.getData().toByteArray()
        }
        0 * responseObserver.onNext(_ as SyncResponse)

        when: "More data is uploaded but fails"
        request = createDataUpload()
        dataUpload = request.getDataUpload()
        unsuccessfulMessageIds.add(dataUpload.getId())
        requestObserver.onNext(request)

        then: "It's still not acknowledged yet but saved as a failed upload"
        1 * jobFileService.updateFile(_ as String, _ as String, _ as Long, _ as byte[]) >> { arguments ->
            assert arguments[0] == jobId
            assert arguments[1] == dataUpload.getPath()
            assert arguments[2] == dataUpload.getStartByte()
            assert arguments[3] == dataUpload.getData().toByteArray()
            throw new IOException("Something went wrong")
        }
        0 * responseObserver.onNext(_ as SyncResponse)

        when: "The maximum number of upload messages have happened"
        request = createDataUpload()
        dataUpload = request.getDataUpload()
        successfulMessageIds.add(dataUpload.getId())
        requestObserver.onNext(request)

        then: "An acknowledgement message is sent with all previous ids"
        1 * jobFileService.updateFile(_ as String, _ as String, _ as Long, _ as byte[]) >> { arguments ->
            assert arguments[0] == jobId
            assert arguments[1] == dataUpload.getPath()
            assert arguments[2] == dataUpload.getStartByte()
            assert arguments[3] == dataUpload.getData().toByteArray()
        }
        1 * responseObserver.onNext(_ as SyncResponse) >> { arguments -> response = (SyncResponse) arguments[0] }
        response != null
        response.hasSyncAck()
        response.getSyncAck().getResultsCount() == successfulMessageIds.size() + unsuccessfulMessageIds.size()
        response.getSyncAck().getResultsCount() == 3
        response.getSyncAck().getResults(0).getId() == successfulMessageIds.get(0)
        response.getSyncAck().getResults(0).getSuccessful()
        response.getSyncAck().getResults(1).getId() == unsuccessfulMessageIds.get(0)
        !response.getSyncAck().getResults(1).getSuccessful()
        response.getSyncAck().getResults(2).getId() == successfulMessageIds.get(1)
        response.getSyncAck().getResults(2).getSuccessful()

        when: "Data is uploaded after an acknowledgement message"
        successfulMessageIds.clear()
        unsuccessfulMessageIds.clear()
        request = createDataUpload()
        dataUpload = request.getDataUpload()
        successfulMessageIds.add(dataUpload.getId())
        requestObserver.onNext(request)

        then: "No acknowledgement is sent but the id buffers have been reset"
        1 * jobFileService.updateFile(_ as String, _ as String, _ as Long, _ as byte[]) >> { arguments ->
            assert arguments[0] == jobId
            assert arguments[1] == dataUpload.getPath()
            assert arguments[2] == dataUpload.getStartByte()
            assert arguments[3] == dataUpload.getData().toByteArray()
        }
        0 * responseObserver.onNext(_ as SyncResponse)

        when: "A delete message is sent"
        request = createDeleteFile()
        deleteFile = request.getDeleteFile()
        requestObserver.onNext(request)
        successfulMessageIds.add(deleteFile.getId())

        then: "It succeeds, no acknowledgement is sent but it's saved in the buffer"
        1 * jobFileService.deleteJobFile(_ as String, _ as String) >> { arguments ->
            assert arguments[0] == jobId
            assert arguments[1] == deleteFile.getPath()
        }
        0 * responseObserver.onNext(_ as SyncResponse)

        when: "The acknowledgement thread fires and data is in the buffers"
        service.executeObserverAcknowledgements()

        then: "An acknowledgement message is sent from the observer"
        1 * responseObserver.onNext(_ as SyncResponse) >> { arguments -> response = (SyncResponse) arguments[0] }
        response != null
        response.hasSyncAck()
        response.getSyncAck().getResultsCount() == successfulMessageIds.size() + unsuccessfulMessageIds.size()
        response.getSyncAck().getResultsCount() == 2
        response.getSyncAck().getResults(0).getId() == successfulMessageIds.get(0)
        response.getSyncAck().getResults(0).getSuccessful()
        response.getSyncAck().getResults(1).getId() == successfulMessageIds.get(1)
        response.getSyncAck().getResults(1).getSuccessful()

        when: "The acknowledgement thread fires again but no further data has been sent"
        successfulMessageIds.clear()
        unsuccessfulMessageIds.clear()
        service.executeObserverAcknowledgements()

        then: "No acknowledgement is sent"
        0 * responseObserver.onNext(_ as SyncResponse)

        when: "A delete message is sent"
        request = createDeleteFile()
        deleteFile = request.getDeleteFile()
        requestObserver.onNext(request)
        unsuccessfulMessageIds.add(deleteFile.getId())

        then: "It fails, no acknowledgement is sent but it's saved in the buffer"
        1 * jobFileService.deleteJobFile(_ as String, _ as String) >> { arguments ->
            assert arguments[0] == jobId
            assert arguments[1] == deleteFile.getPath()
            throw new IOException("Failed to delete due to IO")
        }
        0 * responseObserver.onNext(_ as SyncResponse)

        when: "The acknowledgement thread fires and data is in the buffers"
        service.executeObserverAcknowledgements()

        then: "An acknowledgement message is sent from the observer"
        1 * responseObserver.onNext(_ as SyncResponse) >> { arguments -> response = (SyncResponse) arguments[0] }
        response != null
        response.hasSyncAck()
        response.getSyncAck().getResultsCount() == successfulMessageIds.size() + unsuccessfulMessageIds.size()
        response.getSyncAck().getResultsCount() == 1
        response.getSyncAck().getResults(0).getId() == unsuccessfulMessageIds.get(0)
        !response.getSyncAck().getResults(0).getSuccessful()

        when: "A sync complete message is sent"
        requestObserver.onNext(createSyncComplete())

        then: "Directory state is calculated and cleanup is performed"
        1 * jobFileService.getJobDirectoryFileState(jobId, true) >> Sets.newHashSet()
        service.jobSyncRequestObservers.isEmpty()

//        when: "The connection throws an error"
//        requestObserver.onError(Mock(Exception))
//
//        then: "Cleanup is attempted"
//        service.jobSyncRequestObservers.isEmpty()

        when: "The connection is marked as completed"
        requestObserver.onCompleted()

        then: "Cleanup is performed but nothing happens as it's already been invoked once"
        service.jobSyncRequestObservers.isEmpty()

        when: "The service cleanup method is invoked before it is destroyed"
        service.cleanup()

        then: "The acknowledgement thread is cancelled"
        1 * scheduledFuture.isDone() >> false
        1 * scheduledFuture.cancel(false) >> true
    }

    def "Can add and remove observer"() {
        def jobFileSyncRpcProperties = new JobFileSyncRpcProperties()
        JobFileService jobFileService = Mock()
        TaskScheduler taskScheduler = Mock()
        def service = new GRpcJobFileSyncServiceImpl(jobFileSyncRpcProperties, jobFileService, taskScheduler)
        def observer = Mock(GRpcJobFileSyncServiceImpl.JobFileSyncObserver)
        def jobId = UUID.randomUUID().toString()

        when:
        service.addJobFileSyncObserver(observer)

        then:
        1 * observer.getJobId() >> Optional.empty()
        thrown(IllegalArgumentException)

        when:
        service.addJobFileSyncObserver(observer)

        then:
        1 * observer.getJobId() >> Optional.of(jobId)
        service.jobSyncRequestObservers.containsKey(jobId)

        when:
        service.onAgentDetached(UUID.randomUUID().toString())

        then:
        0 * observer.cleanup()

        when:
        service.onAgentDetached(jobId)

        then:
        1 * observer.cleanup()
    }

    SyncRequest createDataUpload() {
        def data = UUID.randomUUID().toString()
        def startByte = RandomSuppliers.LONG.get()
        def path = UUID.randomUUID().toString()
        def id = UUID.randomUUID().toString()

        return SyncRequest.newBuilder().setDataUpload(
                DataUpload
                        .newBuilder()
                        .setId(id)
                        .setPath(path)
                        .setStartByte(startByte)
                        .setData(ByteString.copyFrom(data, UTF_8))
                        .build()
        ).build()
    }

    SyncRequest createSyncComplete() {
        def jobDirectoryState = JobDirectoryState
                .newBuilder()
                .setIncludesMd5(true)
                .addFiles(createJobFileState())
                .addFiles(createJobFileState())
                .addFiles(createJobFileState())
                .build()

        def syncComplete = SyncComplete.newBuilder().setFinalAgentDirectoryState(jobDirectoryState).build()

        return SyncRequest.newBuilder().setSyncComplete(syncComplete).build()
    }

    com.netflix.genie.proto.JobFileState createJobFileState() {
        com.netflix.genie.proto.JobFileState
                .newBuilder()
                .setPath(UUID.randomUUID().toString())
                .setSize(RandomSuppliers.LONG.get())
                .setMd5(UUID.randomUUID().toString())
                .build()
    }

    SyncRequest createDeleteFile() {
        def id = UUID.randomUUID().toString()
        def path = UUID.randomUUID().toString()

        def deleteFile = DeleteFile.newBuilder().setId(id).setPath(path).build()

        return SyncRequest.newBuilder().setDeleteFile(deleteFile).build()
    }
}
