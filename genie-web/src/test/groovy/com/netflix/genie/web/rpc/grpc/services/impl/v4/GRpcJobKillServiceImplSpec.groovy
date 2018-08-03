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

import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.exceptions.GenieServerException
import com.netflix.genie.proto.*
import com.netflix.genie.web.services.JobSearchService
import io.grpc.stub.StreamObserver
import spock.lang.Specification


/**
 * Specifications for the {@link GRpcJobKillServiceImpl} class.
 *
 * @author standon
 * @since 4.0.0
 */
class GRpcJobKillServiceImplSpec extends Specification {

    GRpcJobKillServiceImpl service
    String jobId
    JobKillRegistrationRequest request
    JobKillRegistrationResponse response
    JobSearchService jobSearchService = Mock()
    StreamObserver<SyncResponse> responseObserver = Mock()

    void setup() {
        jobId = UUID.randomUUID().toString()
        request = JobKillRegistrationRequest.newBuilder().setJobId(jobId).build()
        response = JobKillRegistrationResponse.newBuilder().build()
        service = new GRpcJobKillServiceImpl(jobSearchService)
    }

    def "Can kill unfinished jobs and clean up response observer"() {

        when:
        service.registerForKillNotification(request, responseObserver)

        then:
        noExceptionThrown()

        when:
        service.killJob(jobId, "testing")

        then:
        1 * jobSearchService.getJobStatus(jobId) >> JobStatus.RUNNING
        1 * responseObserver.onNext(response)
        1 * responseObserver.onCompleted()

        when:
        service.killJob(jobId, "testing")

        then:
        thrown(GenieServerException)
    }

    def "For finished job do not interact with agent. Clean up response observer"() {

        when:
        service.registerForKillNotification(request, responseObserver)

        then:
        noExceptionThrown()

        when:
        service.killJob(jobId, "testing")

        then:
        1 * jobSearchService.getJobStatus(jobId) >> JobStatus.SUCCEEDED
        0 * responseObserver.onNext(response)
        0 * responseObserver.onCompleted()

        when:
        service.killJob(jobId, "testing")

        then:
        thrown(GenieServerException)
    }
}
