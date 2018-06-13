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

package com.netflix.genie.agent.execution.statemachine.actions

import com.netflix.genie.agent.AgentMetadata
import com.netflix.genie.agent.cli.ArgumentDelegates
import com.netflix.genie.agent.cli.JobRequestConverter
import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.exceptions.JobIdUnavailableException
import com.netflix.genie.agent.execution.exceptions.JobReservationException
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.test.categories.UnitTest
import org.assertj.core.util.Sets
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import javax.validation.ConstraintViolation

@Category(UnitTest)
class ResolveJobSpecificationActionSpec extends Specification {
    ExecutionContext executionContext
    ArgumentDelegates.JobRequestArguments arguments
    AgentJobService agentJobService
    JobRequestConverter converter
    AgentMetadata agentMetadata
    ResolveJobSpecificationAction action
    AgentJobRequest request
    JobSpecification spec
    String id
    String hostname = "host.foo.com"
    String version = "1.2.3"
    int pid = 1234

    @Rule
    TemporaryFolder temporaryFolder

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.arguments = Mock(ArgumentDelegates.JobRequestArguments)
        this.agentJobService = Mock(AgentJobService)
        this.converter = Mock(JobRequestConverter)
        this.agentMetadata = Mock(AgentMetadata)
        this.action = new ResolveJobSpecificationAction(
                executionContext,
                arguments,
                agentJobService,
                agentMetadata,
                converter
        )
        this.request = Mock(AgentJobRequest)
        this.spec = Mock(JobSpecification)

        this.temporaryFolder.create()
        this.id = UUID.randomUUID().toString()
    }

    void cleanup() {
    }

    def "API Job -- Successful"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> hostname
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> String.valueOf(pid)
        1 * arguments.isJobRequestedViaAPI() >> true
        1 * executionContext.setCurrentJobStatus(JobStatus.ACCEPTED)
        1 * arguments.getJobId() >> id
        1 * agentJobService.getJobSpecification(id) >> spec

        1 * agentJobService.claimJob(id, _ as AgentClientMetadata) >> {
            args ->
                AgentClientMetadata agentClientMetadata = args[1] as AgentClientMetadata
                assert agentClientMetadata != null
                assert agentClientMetadata.getHostname().get() == hostname
                assert agentClientMetadata.getVersion().get() == version
                assert agentClientMetadata.getPid().get() == pid
        }
        1 * executionContext.setCurrentJobStatus(JobStatus.CLAIMED)
        1 * executionContext.setJobSpecification(spec)
        1 * executionContext.setClaimedJobId(id)

        expect:
        event == Events.RESOLVE_JOB_SPECIFICATION_COMPLETE
    }

    def "Successful -- API job"() {
        Exception exception = new JobSpecificationResolutionException("...")
        when:
        action.executeStateAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> hostname
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> String.valueOf(pid)
        1 * arguments.isJobRequestedViaAPI() >> true
        1 * executionContext.setCurrentJobStatus(JobStatus.ACCEPTED)
        1 * arguments.getJobId() >> id
        1 * agentJobService.getJobSpecification(id) >> { throw exception }
        Throwable e = thrown(RuntimeException)

        expect:
        e.getCause() == exception
    }

    def "CLI job -- Successful"() {
        AgentJobRequest agentJobRequest = Mock(AgentJobRequest)

        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> hostname
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> String.valueOf(pid)
        1 * arguments.isJobRequestedViaAPI() >> false
        1 * converter.agentJobRequestArgsToDTO(arguments) >> agentJobRequest
        1 * agentJobService.reserveJobId(agentJobRequest, _ as AgentClientMetadata) >> {
            args ->
                AgentClientMetadata agentClientMetadata = args[1] as AgentClientMetadata
                assert agentClientMetadata != null
                assert agentClientMetadata.getHostname().get() == hostname
                assert agentClientMetadata.getVersion().get() == version
                assert agentClientMetadata.getPid().get() == pid
                return id
        }
        1 * executionContext.setCurrentJobStatus(JobStatus.RESERVED)
        1 * agentJobService.resolveJobSpecification(id) >> spec
        1 * executionContext.setCurrentJobStatus(JobStatus.RESOLVED)
        1 * agentJobService.claimJob(id, _ as AgentClientMetadata) >> {
            args ->
                AgentClientMetadata agentClientMetadata = args[1] as AgentClientMetadata
                assert agentClientMetadata != null
                assert agentClientMetadata.getHostname().get() == hostname
                assert agentClientMetadata.getVersion().get() == version
                assert agentClientMetadata.getPid().get() == pid
        }
        1 * executionContext.setCurrentJobStatus(JobStatus.CLAIMED)
        1 * executionContext.setJobSpecification(spec)
        1 * executionContext.setClaimedJobId(id)

        expect:
        event == Events.RESOLVE_JOB_SPECIFICATION_COMPLETE
    }

    def "CLI job -- Conversion exception"() {
        ConstraintViolation constraintViolation = Mock()
        Exception exception = new JobRequestConverter.ConversionException(Sets.<ConstraintViolation<AgentJobRequest>>newHashSet([constraintViolation]))

        when:
        action.executeStateAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> hostname
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> String.valueOf(pid)
        1 * arguments.isJobRequestedViaAPI() >> false
        1 * converter.agentJobRequestArgsToDTO(arguments) >> { throw exception }
        Throwable e = thrown(RuntimeException)

        expect:
        e.getCause() == exception
    }

    def "CLI job -- Reservation exception, id not available"() {
        Exception exception = new JobIdUnavailableException("...")
        AgentJobRequest agentJobRequest = Mock(AgentJobRequest)

        when:
        action.executeStateAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> hostname
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> String.valueOf(pid)
        1 * arguments.isJobRequestedViaAPI() >> false
        1 * converter.agentJobRequestArgsToDTO(arguments) >> agentJobRequest
        1 * agentJobService.reserveJobId(agentJobRequest, _ as AgentClientMetadata) >> {
            args ->
                AgentClientMetadata agentClientMetadata = args[1] as AgentClientMetadata
                assert agentClientMetadata != null
                assert agentClientMetadata.getHostname().get() == hostname
                assert agentClientMetadata.getVersion().get() == version
                assert agentClientMetadata.getPid().get() == pid
                throw exception
        }
        Throwable e = thrown(RuntimeException)

        expect:
        e.getCause() == exception
    }

    def "CLI job -- Reservation exception"() {
        Exception exception = new JobReservationException("...")
        AgentJobRequest agentJobRequest = Mock(AgentJobRequest)

        when:
        action.executeStateAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> hostname
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> String.valueOf(pid)
        1 * arguments.isJobRequestedViaAPI() >> false
        1 * converter.agentJobRequestArgsToDTO(arguments) >> agentJobRequest
        1 * agentJobService.reserveJobId(agentJobRequest, _ as AgentClientMetadata) >> {
            args ->
                AgentClientMetadata agentClientMetadata = args[1] as AgentClientMetadata
                assert agentClientMetadata != null
                assert agentClientMetadata.getHostname().get() == hostname
                assert agentClientMetadata.getVersion().get() == version
                assert agentClientMetadata.getPid().get() == pid
                return id
        }
        1 * executionContext.setCurrentJobStatus(JobStatus.RESERVED)
        1 * agentJobService.resolveJobSpecification(id) >> spec
        1 * executionContext.setCurrentJobStatus(JobStatus.RESOLVED)
        1 * agentJobService.claimJob(id, _ as AgentClientMetadata) >> {
            args ->
                AgentClientMetadata agentClientMetadata = args[1] as AgentClientMetadata
                assert agentClientMetadata != null
                assert agentClientMetadata.getHostname().get() == hostname
                assert agentClientMetadata.getVersion().get() == version
                assert agentClientMetadata.getPid().get() == pid
                throw exception
        }
        Throwable e = thrown(RuntimeException)

        expect:
        e.getCause() == exception
    }

    def "CLI job -- Resolution Exception"() {
        Exception exception = new JobSpecificationResolutionException("...")
        AgentJobRequest agentJobRequest = Mock(AgentJobRequest)

        when:
        action.executeStateAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> hostname
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> String.valueOf(pid)
        1 * arguments.isJobRequestedViaAPI() >> false
        1 * converter.agentJobRequestArgsToDTO(arguments) >> agentJobRequest
        1 * agentJobService.reserveJobId(agentJobRequest, _ as AgentClientMetadata) >> {
            args ->
                AgentClientMetadata agentClientMetadata = args[1] as AgentClientMetadata
                assert agentClientMetadata != null
                assert agentClientMetadata.getHostname().get() == hostname
                assert agentClientMetadata.getVersion().get() == version
                assert agentClientMetadata.getPid().get() == pid
                return id
        }
        1 * executionContext.setCurrentJobStatus(JobStatus.RESERVED)
        1 * agentJobService.resolveJobSpecification(id) >> { throw exception }
        Throwable e = thrown(RuntimeException)

        expect:
        e.getCause() == exception
    }

    def "Claim job exception"() {
        Exception exception = new JobReservationException("...")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> hostname
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> String.valueOf(pid)
        1 * arguments.isJobRequestedViaAPI() >> true
        1 * executionContext.setCurrentJobStatus(JobStatus.ACCEPTED)
        1 * arguments.getJobId() >> id
        1 * agentJobService.getJobSpecification(id) >> spec

        1 * agentJobService.claimJob(id, _ as AgentClientMetadata) >> {
            args ->
                AgentClientMetadata agentClientMetadata = args[1] as AgentClientMetadata
                assert agentClientMetadata != null
                assert agentClientMetadata.getHostname().get() == hostname
                assert agentClientMetadata.getVersion().get() == version
                assert agentClientMetadata.getPid().get() == pid
                throw exception
        }
        Throwable e = thrown(RuntimeException)

        expect:
        e.getCause() == exception
    }
}
