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

import com.netflix.genie.agent.cli.ArgumentDelegates
import com.netflix.genie.agent.cli.JobRequestConverter
import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException
import com.netflix.genie.agent.execution.services.AgentJobSpecificationService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.dto.v4.AgentJobRequest
import com.netflix.genie.common.dto.v4.JobSpecification
import spock.lang.Specification

class ResolveJobSpecificationActionSpec extends Specification {
    ExecutionContext executionContext
    ArgumentDelegates.JobRequestArguments arguments
    AgentJobSpecificationService service
    JobRequestConverter converter
    ResolveJobSpecificationAction action
    AgentJobRequest request
    JobSpecification spec
    JobSpecificationResolutionException exception

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.arguments = Mock(ArgumentDelegates.JobRequestArguments)
        this.service = Mock(AgentJobSpecificationService)
        this.converter = Mock(JobRequestConverter)
        this.action = new ResolveJobSpecificationAction(
                executionContext,
                arguments,
                service,
                converter
        )
        this.request = Mock(AgentJobRequest)
        this.spec = Mock(JobSpecification)
        this.exception = new JobSpecificationResolutionException("error")
    }

    void cleanup() {
    }

    def "ExecuteStateAction"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * converter.agentJobRequestArgsToDTO(arguments) >> request
        1 * service.resolveJobSpecification(request) >> spec
        1 * executionContext.setJobSpecification(spec)

        event == Events.RESOLVE_JOB_SPECIFICATION_COMPLETE
    }

    def "ExecuteStateAction with service error"() {
        setup:

        when:
        action.executeStateAction(executionContext)

        then:
        1 * converter.agentJobRequestArgsToDTO(arguments) >> request
        1 * service.resolveJobSpecification(request) >> { throw exception }
        def e = thrown(RuntimeException)
        e.getCause() == exception
    }
}
