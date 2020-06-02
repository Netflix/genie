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
package com.netflix.genie.agent.cli

import com.fasterxml.jackson.databind.JsonNode
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest
import com.netflix.genie.common.external.dtos.v4.Criterion
import org.assertj.core.util.Sets
import spock.lang.Specification
import spock.lang.Unroll

import javax.validation.ConstraintViolation
import javax.validation.Validator

class JobRequestConverterSpec extends Specification {

    JobRequestConverter converter
    Validator validator
    MainCommandArguments mainCommandArguments

    void setup() {
        validator = Mock(Validator)
        converter = new JobRequestConverter(validator)
        mainCommandArguments = new MainCommandArguments()
    }

    def "Convert with defaults"() {
        setup:
        ArgumentDelegates.JobRequestArguments jobRequestArgs = new JobRequestArgumentsImpl(mainCommandArguments)
        AgentJobRequest jobRequest

        when:
        jobRequest = converter.agentJobRequestArgsToDTO(jobRequestArgs)

        then:
        jobRequest.getRequestedAgentConfig().getTimeoutRequested() == Optional.ofNullable(jobRequestArgs.getTimeout())
        jobRequest.getCommandArgs().isEmpty()
        jobRequest.getRequestedAgentConfig().isInteractive() == jobRequestArgs.isInteractive()
        jobRequest.getRequestedAgentConfig().getRequestedJobDirectoryLocation() == Optional.ofNullable(jobRequestArgs.getJobDirectoryLocation())
        jobRequest.getRequestedAgentConfig().isArchivingDisabled() == jobRequestArgs.isArchivingDisabled()
        jobRequest.getCriteria() != null
        jobRequest.getCriteria().getApplicationIds() == jobRequestArgs.getApplicationIds()
        jobRequest.getCriteria().getClusterCriteria() == jobRequestArgs.getClusterCriteria()
        jobRequest.getCriteria().getCommandCriterion() == jobRequestArgs.getCommandCriterion()
        jobRequest.getMetadata().getEmail() == Optional.ofNullable(jobRequestArgs.getEmail())
        jobRequest.getMetadata().getGrouping() == Optional.ofNullable(jobRequestArgs.getGrouping())
        jobRequest.getMetadata().getGroupingInstance() == Optional.ofNullable(jobRequestArgs.getGroupingInstance())
        jobRequest.getMetadata().getDescription() == Optional.ofNullable(jobRequestArgs.getJobDescription())
        jobRequest.getMetadata().getTags() == jobRequestArgs.getJobTags()
        jobRequest.getMetadata().getVersion() != null
        jobRequest.getMetadata().getName() == jobRequestArgs.getJobName()
        jobRequest.getMetadata().getMetadata() == Optional.ofNullable(jobRequestArgs.getJobMetadata())
        jobRequest.getMetadata().getGroup() == Optional.ofNullable(null)
        jobRequest.getMetadata().getUser() == System.getProperty('user.name')
        jobRequest.getResources().getConfigs().isEmpty()
        jobRequest.getResources().getDependencies().isEmpty()
        !jobRequest.getResources().getSetupFile().isPresent()
        validator.validate(_ as AgentJobRequest) >> Sets.newHashSet()
    }

    def "Convert with non defaults"() {
        setup:
        ArgumentDelegates.JobRequestArguments jobRequestArgs = Mock()
        AgentJobRequest jobRequest
        Criterion clusterCriterion1 = Mock()
        Criterion clusterCriterion2 = Mock()
        Criterion commandCriterion = Mock()
        JsonNode metadata = Mock()

        when:
        jobRequest = converter.agentJobRequestArgsToDTO(jobRequestArgs)

        then:
        1 * jobRequestArgs.getTimeout() >> 10
        jobRequest.getRequestedAgentConfig().getTimeoutRequested().get() == 10
        1 * jobRequestArgs.getCommandArguments() >> ["foo", "bar"].asList()
        jobRequest.getCommandArgs() == ["'foo' 'bar'"].asList()
        1 * jobRequestArgs.isInteractive() >> true
        jobRequest.getRequestedAgentConfig().isInteractive()
        1 * jobRequestArgs.getJobDirectoryLocation() >> new File("/tmp")
        jobRequest.getRequestedAgentConfig().getRequestedJobDirectoryLocation() == Optional.of(new File("/tmp"))
        1 * jobRequestArgs.getClusterCriteria() >> [clusterCriterion1, clusterCriterion2].asList()
        jobRequest.getCriteria().getClusterCriteria() == [clusterCriterion1, clusterCriterion2].asList()
        1 * jobRequestArgs.getCommandCriterion() >> commandCriterion
        jobRequest.getCriteria().getCommandCriterion() == commandCriterion
        1 * jobRequestArgs.getApplicationIds() >> ["app1, app2"].asList()
        jobRequest.getCriteria().getApplicationIds() == ["app1, app2"].asList()
        1 * jobRequestArgs.getEmail() >> "foo@bar.com"
        jobRequest.getMetadata().getEmail() == Optional.of("foo@bar.com")
        1 * jobRequestArgs.getGrouping() >> "g"
        jobRequest.getMetadata().getGrouping() == Optional.of("g")
        1 * jobRequestArgs.getGroupingInstance() >> "gi"
        jobRequest.getMetadata().getGroupingInstance() == Optional.of("gi")
        1 * jobRequestArgs.getJobDescription() >> "d"
        jobRequest.getMetadata().getDescription() == Optional.of("d")
        1 * jobRequestArgs.getJobTags() >> Sets.newHashSet(["t1", "t2"])
        jobRequest.getMetadata().getTags() == Sets.newHashSet(["t1", "t2"])
        1 * jobRequestArgs.getJobVersion() >> "1.2.3"
        jobRequest.getMetadata().getVersion() == "1.2.3"
        1 * jobRequestArgs.getJobName() >> "n"
        jobRequest.getMetadata().getName() == "n"
        1 * jobRequestArgs.getJobMetadata() >> metadata
        jobRequest.getMetadata().getMetadata() == Optional.of(metadata)
        1 * jobRequestArgs.getUser() >> "u"
        jobRequest.getMetadata().getUser() == "u"
        1 * jobRequestArgs.getJobDependencies() >> ["d1", "d2"]
        jobRequest.getResources().getDependencies() == Sets.newHashSet(["d1", "d2"])
        1 * jobRequestArgs.getJobConfigurations() >> ["c1", "c2"]
        jobRequest.getResources().getConfigs() == Sets.newHashSet(["c1", "c2"])
        1 * jobRequestArgs.getJobSetup() >> "setup.sh"
        jobRequest.getResources().getSetupFile() == Optional.of("setup.sh")
        1 * jobRequestArgs.isArchivingDisabled() >> true
        jobRequest.getRequestedAgentConfig().isArchivingDisabled()
        1 * validator.validate(_ as AgentJobRequest) >> Sets.newHashSet()
    }

    @Unroll
    def "Convert arguments: #providedJobArguments"() {

        setup:
        ArgumentDelegates.JobRequestArguments jobRequestArgs = Spy(new JobRequestArgumentsImpl(mainCommandArguments))
        AgentJobRequest jobRequest

        when:
        jobRequest = converter.agentJobRequestArgsToDTO(jobRequestArgs)

        then:
        1 * jobRequestArgs.getCommandArguments() >> providedJobArguments
        1 * validator.validate(_ as AgentJobRequest) >> Sets.newHashSet()

        expect:
        jobRequest.getCommandArgs() == expectedJobArguments

        where:
        providedJobArguments                    | expectedJobArguments
        []                                      | []
        ["foo", "bar"]                          | ["'foo' 'bar'"]
        ["d'ho!"]                               | ["'d'\\''ho!'"]
        ["'blah blah'"]                         | ["''\\''blah blah'\\'''"]
        ["table['column']"]                     | ["'table['\\''column'\\'']'"]
        ["tables=t1, t2", "columns=\"c1, c2\""] | ["'tables=t1, t2' 'columns=\"c1, c2\"'"]
        ["--conf", "variable=\$foo"]            | ["'--conf' 'variable=\$foo'"]
        ["\t\tblah"]                            | ["'\t\tblah'"]
    }

    def "Convert fail validation"() {

        setup:
        ArgumentDelegates.JobRequestArguments jobRequestArgs = new JobRequestArgumentsImpl(mainCommandArguments)
        ConstraintViolation<AgentJobRequest> violation = Mock()

        when:
        converter.agentJobRequestArgsToDTO(jobRequestArgs)

        then:
        1 * validator.validate(_ as AgentJobRequest) >> Sets.newHashSet([violation])
        1 * violation.getMessage() >> "NO!"
        thrown(JobRequestConverter.ConversionException)
    }
}
