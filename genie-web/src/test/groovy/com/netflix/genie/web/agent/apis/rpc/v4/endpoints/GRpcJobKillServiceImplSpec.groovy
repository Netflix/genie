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
package com.netflix.genie.web.agent.apis.rpc.v4.endpoints

import com.netflix.genie.common.exceptions.GenieServerException
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.proto.JobKillRegistrationRequest
import com.netflix.genie.proto.JobKillRegistrationResponse
import com.netflix.genie.web.agent.services.AgentRoutingService
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.exceptions.checked.NotFoundException
import com.netflix.genie.web.services.RequestForwardingService
import io.grpc.stub.StreamObserver
import spock.lang.Specification

import javax.servlet.http.HttpServletRequest

/**
 * Specifications for the {@link GRpcJobKillServiceImpl} class.
 *
 * @author standon
 */
class GRpcJobKillServiceImplSpec extends Specification {

    GRpcJobKillServiceImpl serviceSpy
    String jobId
    String reason
    String remoteHost
    JobKillRegistrationRequest request
    JobKillRegistrationResponse response
    PersistenceService persistenceService
    AgentRoutingService agentRoutingService
    RequestForwardingService requestForwardingService
    StreamObserver<JobKillRegistrationResponse> responseObserver = Mock()
    HttpServletRequest servletRequest

    void setup() {
        this.persistenceService = Mock(PersistenceService)
        this.agentRoutingService = Mock(AgentRoutingService)
        this.requestForwardingService = Mock(RequestForwardingService)
        this.jobId = UUID.randomUUID().toString()
        this.reason = UUID.randomUUID().toString()
        this.remoteHost = UUID.randomUUID().toString()
        this.request = JobKillRegistrationRequest.newBuilder().setJobId(this.jobId).build()
        this.response = JobKillRegistrationResponse.newBuilder().build()
        def dataServices = Mock(DataServices) {
            getPersistenceService() >> this.persistenceService
        }
        def service = new GRpcJobKillServiceImpl(dataServices, this.agentRoutingService, this.requestForwardingService)

        this.serviceSpy = Spy(service);
        this.servletRequest = Mock(HttpServletRequest)
    }

    def "Can register for kill notification"() {
        setup:
        serviceSpy.isStreamObserverCancelled(_) >> false

        when:
        this.serviceSpy.registerForKillNotification(this.request, this.responseObserver)

        then:
        noExceptionThrown()
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 1
        this.serviceSpy.getParkedJobKillResponseObservers().get(this.jobId) == this.responseObserver

        when: "A new registration occurs for the same job id"
        this.serviceSpy.registerForKillNotification(this.request, Mock(StreamObserver))

        then: "The old one is removed and closed"
        noExceptionThrown()
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 1
        this.serviceSpy.getParkedJobKillResponseObservers().get(this.jobId) != this.responseObserver
        1 * this.responseObserver.onCompleted()
    }

    def "Kill logic works as expected"() {
        setup:
        serviceSpy.isStreamObserverCancelled(_) >> false

        when: "Job doesn't exist"
        this.serviceSpy.killJob(this.jobId, this.reason, null)

        then: "Expected exception is thrown"
        1 * this.persistenceService.getJobStatus(this.jobId) >> {
            throw new NotFoundException()
        }
        thrown(GenieJobNotFoundException)

        when: "The job is already finished"
        this.serviceSpy.killJob(this.jobId, this.reason, this.servletRequest)

        then: "Nothing needs to be done"
        1 * this.persistenceService.getJobStatus(this.jobId) >> JobStatus.SUCCEEDED
        noExceptionThrown()
        0 * this.persistenceService.updateJobStatus(_ as String, _ as JobStatus, _ as JobStatus, _ as String)
        0 * this.agentRoutingService.isAgentConnectionLocal(this.jobId)

        when: "The job is active, the agent isn't yet started but status has changed since initial call"
        this.serviceSpy.killJob(this.jobId, this.reason, null)

        then: "Only try to set killed in database but is rejected and exception is thrown"
        1 * this.persistenceService.getJobStatus(this.jobId) >> JobStatus.RESERVED
        1 * this.persistenceService.updateJobStatus(this.jobId, JobStatus.RESERVED, JobStatus.KILLED, this.reason) >> {
            throw new GenieInvalidStatusException("whoops")
        }
        0 * this.agentRoutingService.isAgentConnectionLocal(this.jobId)
        thrown(GenieInvalidStatusException)

        when: "The job is active, the agent isn't yet started but for some reason db can't find job"
        this.serviceSpy.killJob(this.jobId, this.reason, null)

        then: "Correct exception is thrown"
        1 * this.persistenceService.getJobStatus(this.jobId) >> JobStatus.ACCEPTED
        1 * this.persistenceService.updateJobStatus(this.jobId, JobStatus.ACCEPTED, JobStatus.KILLED, this.reason) >> {
            throw new NotFoundException("whoops")
        }
        0 * this.agentRoutingService.isAgentConnectionLocal(this.jobId)
        thrown(GenieJobNotFoundException)

        when: "The job is active, the agent isn't yet started and the db can find the job"
        this.serviceSpy.killJob(this.jobId, this.reason, this.servletRequest)

        then: "The database is updated and no exception is thrown"
        1 * this.persistenceService.getJobStatus(this.jobId) >> JobStatus.RESOLVED
        1 * this.persistenceService.updateJobStatus(this.jobId, JobStatus.RESOLVED, JobStatus.KILLED, this.reason)
        0 * this.agentRoutingService.isAgentConnectionLocal(this.jobId)
        noExceptionThrown()

        when: "The job is active, the agent is connected, the job is local but no observer"
        this.serviceSpy.killJob(this.jobId, this.reason, this.servletRequest)

        then: "Correct exception is thrown"
        1 * this.persistenceService.getJobStatus(this.jobId) >> JobStatus.CLAIMED
        0 * this.persistenceService.updateJobStatus(_ as String, _ as JobStatus, _ as JobStatus, _ as String)
        1 * this.agentRoutingService.isAgentConnectionLocal(this.jobId) >> true
        0 * this.responseObserver.onNext(_ as JobKillRegistrationResponse)
        0 * this.responseObserver.onCompleted()
        thrown(GenieServerException)

        when: "The job is active, the agent is connected, and there is an observer"
        this.serviceSpy.registerForKillNotification(this.request, this.responseObserver)
        this.serviceSpy.killJob(this.jobId, this.reason, null)

        then: "Kill message is sent and the observer is removed"
        1 * this.persistenceService.getJobStatus(this.jobId) >> JobStatus.INIT
        0 * this.persistenceService.updateJobStatus(_ as String, _ as JobStatus, _ as JobStatus, _ as String)
        1 * this.agentRoutingService.isAgentConnectionLocal(this.jobId) >> true
        1 * this.responseObserver.onNext(_ as JobKillRegistrationResponse)
        1 * this.responseObserver.onCompleted()
        this.serviceSpy.getParkedJobKillResponseObservers().isEmpty()

        when: "The job is active, the agent is connected to another server but we can't determine where"
        this.serviceSpy.killJob(this.jobId, this.reason, this.servletRequest)

        then: "An exception is thrown"
        1 * this.persistenceService.getJobStatus(this.jobId) >> JobStatus.RUNNING
        0 * this.persistenceService.updateJobStatus(_ as String, _ as JobStatus, _ as JobStatus, _ as String)
        1 * this.agentRoutingService.isAgentConnectionLocal(this.jobId) >> false
        0 * this.responseObserver.onNext(_ as JobKillRegistrationResponse)
        0 * this.responseObserver.onCompleted()
        1 * this.agentRoutingService.getHostnameForAgentConnection(this.jobId) >> Optional.empty()
        thrown(GenieServerException)

        when: "The job is active and we try to forward request"
        this.serviceSpy.killJob(this.jobId, this.reason, this.servletRequest)

        then: "No exception is thrown"
        1 * this.persistenceService.getJobStatus(this.jobId) >> JobStatus.RUNNING
        0 * this.persistenceService.updateJobStatus(_ as String, _ as JobStatus, _ as JobStatus, _ as String)
        1 * this.agentRoutingService.isAgentConnectionLocal(this.jobId) >> false
        0 * this.responseObserver.onNext(_ as JobKillRegistrationResponse)
        0 * this.responseObserver.onCompleted()
        1 * this.agentRoutingService.getHostnameForAgentConnection(this.jobId) >> Optional.of(this.remoteHost)
        1 * this.requestForwardingService.kill(this.remoteHost, this.jobId, this.servletRequest)
        noExceptionThrown()
    }

    def "can clean up orphans"() {
        setup:
        serviceSpy.isStreamObserverCancelled(_) >> false

        def jobId0 = UUID.randomUUID().toString()
        def jobId1 = UUID.randomUUID().toString()
        def jobId2 = UUID.randomUUID().toString()

        def request0 = JobKillRegistrationRequest.newBuilder().setJobId(jobId0).build()
        def request1 = JobKillRegistrationRequest.newBuilder().setJobId(jobId1).build()
        def request2 = JobKillRegistrationRequest.newBuilder().setJobId(jobId2).build()

        def jobObserver0 = Mock(StreamObserver)
        def jobObserver1 = Mock(StreamObserver)
        def jobObserver2 = Mock(StreamObserver)

        when: "Nothing is registered"
        this.serviceSpy.cleanupOrphanedObservers()

        then: "Nothing happens"
        this.serviceSpy.getParkedJobKillResponseObservers().isEmpty()

        when: "Jobs are registered"
        this.serviceSpy.registerForKillNotification(request0, jobObserver0)
        this.serviceSpy.registerForKillNotification(request1, jobObserver1)
        this.serviceSpy.registerForKillNotification(request2, jobObserver2)

        then: "They're saved in map"
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 3
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId0) == jobObserver0
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId1) == jobObserver1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId2) == jobObserver2

        when: "All jobs still attached locally"
        this.serviceSpy.cleanupOrphanedObservers()

        then: "Nothing happens"
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId0) >> true
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId1) >> true
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId2) >> true
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 3
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId0) == jobObserver0
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId1) == jobObserver1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId2) == jobObserver2
        0 * jobObserver0.onCompleted()
        0 * jobObserver1.onCompleted()
        0 * jobObserver2.onCompleted()

        when: "The jobs switch servers or complete"
        this.serviceSpy.cleanupOrphanedObservers()

        then: "It is removed from the map and the observer is completed"
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId0) >> false
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId1) >> true
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId2) >> false
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId0) == null
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId1) == jobObserver1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId2) == null
        1 * jobObserver0.onCompleted()
        0 * jobObserver1.onCompleted()
        1 * jobObserver2.onCompleted()
        noExceptionThrown()
    }

    def "cleans up orphans but does not complete cancelled"() {
        setup:
        serviceSpy.isStreamObserverCancelled(_) >> true

        def jobId0 = UUID.randomUUID().toString()
        def jobId1 = UUID.randomUUID().toString()
        def jobId2 = UUID.randomUUID().toString()

        def request0 = JobKillRegistrationRequest.newBuilder().setJobId(jobId0).build()
        def request1 = JobKillRegistrationRequest.newBuilder().setJobId(jobId1).build()
        def request2 = JobKillRegistrationRequest.newBuilder().setJobId(jobId2).build()

        def jobObserver0 = Mock(StreamObserver)
        def jobObserver1 = Mock(StreamObserver)
        def jobObserver2 = Mock(StreamObserver)

        when: "Jobs are registered"
        this.serviceSpy.registerForKillNotification(request0, jobObserver0)
        this.serviceSpy.registerForKillNotification(request1, jobObserver1)
        this.serviceSpy.registerForKillNotification(request2, jobObserver2)

        then: "They're saved in map"
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 3
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId0) == jobObserver0
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId1) == jobObserver1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId2) == jobObserver2

        when: "The jobs switch servers or complete"
        this.serviceSpy.cleanupOrphanedObservers()

        then: "It is removed from the map but the observers are cancelled so no onCompleted"
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId0) >> false
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId1) >> true
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId2) >> false
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId0) == null
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId1) == jobObserver1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId2) == null
        0 * jobObserver0.onCompleted()
        0 * jobObserver1.onCompleted()
        0 * jobObserver2.onCompleted()
        noExceptionThrown()
    }

    def "clean up orphans and no exceptions thrown when onComplete throws"() {
        setup:
        serviceSpy.isStreamObserverCancelled(_) >> false

        def jobId0 = UUID.randomUUID().toString()
        def jobId1 = UUID.randomUUID().toString()
        def jobId2 = UUID.randomUUID().toString()

        def request0 = JobKillRegistrationRequest.newBuilder().setJobId(jobId0).build()
        def request1 = JobKillRegistrationRequest.newBuilder().setJobId(jobId1).build()
        def request2 = JobKillRegistrationRequest.newBuilder().setJobId(jobId2).build()

        def jobObserver0 = Mock(StreamObserver)
        def jobObserver1 = Mock(StreamObserver)
        def jobObserver2 = Mock(StreamObserver)

        when: "Jobs are registered"
        this.serviceSpy.registerForKillNotification(request0, jobObserver0)
        this.serviceSpy.registerForKillNotification(request1, jobObserver1)
        this.serviceSpy.registerForKillNotification(request2, jobObserver2)

        then: "They're saved in map"
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 3
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId0) == jobObserver0
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId1) == jobObserver1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId2) == jobObserver2

        when: "The jobs switch servers or complete"
        this.serviceSpy.cleanupOrphanedObservers()

        then: "It is removed from the map and onComplete throws exception we catch"
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId0) >> false
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId1) >> true
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId2) >> false
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId0) == null
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId1) == jobObserver1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId2) == null
        1 * jobObserver0.onCompleted() >> { throw new RuntimeException("Mock exception") }
        0 * jobObserver1.onCompleted()
        1 * jobObserver2.onCompleted() >> { throw new RuntimeException("Mock exception") }
        noExceptionThrown()
    }

    def "clean up orphans and no exceptions thrown when loop body throws"() {
        setup:
        serviceSpy.isStreamObserverCancelled(_) >> false

        def jobId0 = UUID.randomUUID().toString()
        def jobId1 = UUID.randomUUID().toString()
        def jobId2 = UUID.randomUUID().toString()

        def request0 = JobKillRegistrationRequest.newBuilder().setJobId(jobId0).build()
        def request1 = JobKillRegistrationRequest.newBuilder().setJobId(jobId1).build()
        def request2 = JobKillRegistrationRequest.newBuilder().setJobId(jobId2).build()

        def jobObserver0 = Mock(StreamObserver)
        def jobObserver1 = Mock(StreamObserver)
        def jobObserver2 = Mock(StreamObserver)

        when: "Jobs are registered"
        this.serviceSpy.registerForKillNotification(request0, jobObserver0)
        this.serviceSpy.registerForKillNotification(request1, jobObserver1)
        this.serviceSpy.registerForKillNotification(request2, jobObserver2)

        then: "They're saved in map"
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 3
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId0) == jobObserver0
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId1) == jobObserver1
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId2) == jobObserver2

        when: "The jobs switch servers or complete"
        this.serviceSpy.cleanupOrphanedObservers()

        then: "It is removed from the map and onComplete throws exception we catch"
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId0) >> { throw new RuntimeException("Mock exception") }
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId1) >> false
        1 * this.agentRoutingService.isAgentConnectionLocal(jobId2) >> { throw new RuntimeException("Mock exception") }
        this.serviceSpy.getParkedJobKillResponseObservers().size() == 2
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId0) == jobObserver0
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId1) == null
        this.serviceSpy.getParkedJobKillResponseObservers().get(jobId2) == jobObserver2
        0 * jobObserver0.onCompleted()
        1 * jobObserver1.onCompleted()
        0 * jobObserver2.onCompleted()
        noExceptionThrown()
    }
}
