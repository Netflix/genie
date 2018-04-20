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

import com.google.common.collect.Maps
import com.netflix.genie.agent.cli.ArgumentDelegates
import com.netflix.genie.agent.cli.JobRequestConverter
import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.util.GenieObjectMapper
import org.assertj.core.util.Sets
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import javax.validation.ConstraintViolation

class ResolveJobSpecificationActionSpec extends Specification {
    ExecutionContext executionContext
    ArgumentDelegates.JobRequestArguments arguments
    AgentJobService service
    JobRequestConverter converter
    ResolveJobSpecificationAction action
    AgentJobRequest request
    JobSpecification spec
    JobSpecificationResolutionException exception

    @Rule
    TemporaryFolder temporaryFolder

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.arguments = Mock(ArgumentDelegates.JobRequestArguments)
        this.service = Mock(AgentJobService)
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

        this.temporaryFolder.create()
    }

    void cleanup() {
    }

    def "ExecuteStateAction"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * converter.agentJobRequestArgsToDTO(arguments) >> request
        1 * service.resolveJobSpecification(request) >> spec
        1 * executionContext.setJobSpecification(_ as JobSpecification)

        event == Events.RESOLVE_JOB_SPECIFICATION_COMPLETE
    }

    def "ExecuteStateAction with spec file"() {
        setup:
        def specFile = temporaryFolder.newFile()
        def minimalJobSpec =
                new JobSpecification(
                        [],
                        new JobSpecification.ExecutionResource(
                                "my-job",
                                new ExecutionEnvironment(Sets.newHashSet(), Sets.newHashSet(), "file:///foo"),
                        ),
                        new JobSpecification.ExecutionResource(
                                "my-cluster",
                                new ExecutionEnvironment(Sets.newHashSet(), Sets.newHashSet(), "file:///foo"),
                        ),
                        new JobSpecification.ExecutionResource(
                                "my-command",
                                new ExecutionEnvironment(Sets.newHashSet(), Sets.newHashSet(), "file:///foo"),
                        ),
                        [].toList(),
                        Maps.newHashMap(),
                        false,
                        new File("/tmp")
                );
        GenieObjectMapper.getMapper().writeValue(specFile, minimalJobSpec)

        when:
        def event = action.executeStateAction(executionContext)

        then:
        2 * arguments.getJobSpecificationFile() >> specFile
        0 * converter.agentJobRequestArgsToDTO(arguments)
        0 * service.resolveJobSpecification(request)
        1 * executionContext.setJobSpecification(minimalJobSpec)
        event == Events.RESOLVE_JOB_SPECIFICATION_COMPLETE
    }

    def "ExecuteStateAction with nonexistent spec file"() {
        setup:
        def specFile = temporaryFolder.newFile()

        when:
        def event = action.executeStateAction(executionContext)

        then:
        2 * arguments.getJobSpecificationFile() >> specFile
        0 * converter.agentJobRequestArgsToDTO(arguments)
        0 * service.resolveJobSpecification(request)
        0 * executionContext.setJobSpecification(_)
        Throwable e = thrown(RuntimeException)
        IOException.isInstance(e.getCause())
    }

    def "ExecuteStateAction with arguments error"() {
        setup:
        ConstraintViolation<AgentJobRequest> cv = Mock() {
            _ * getMessage() >> "Constraint violated!"
        }
        def conversionException = new JobRequestConverter.ConversionException(Sets.newHashSet([cv]))

        when:
        action.executeStateAction(executionContext)

        then:
        1 * converter.agentJobRequestArgsToDTO(arguments) >> { throw conversionException}
        def e = thrown(RuntimeException)
        e.getCause() == conversionException
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
