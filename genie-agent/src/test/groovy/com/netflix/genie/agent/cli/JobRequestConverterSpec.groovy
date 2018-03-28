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
import com.netflix.genie.common.dto.v4.AgentJobRequest
import com.netflix.genie.common.dto.v4.Criterion
import com.netflix.genie.test.categories.UnitTest
import org.assertj.core.util.Sets
import org.junit.experimental.categories.Category
import spock.lang.Specification

import javax.validation.ConstraintViolation
import javax.validation.Validator

@Category(UnitTest.class)
class JobRequestConverterSpec extends Specification {

    JobRequestConverter converter
    Validator validator

    void setup() {
        validator = Mock(Validator)
        converter = new JobRequestConverter(validator)
    }

    def "Convert with defaults"() {

        setup:
        ArgumentDelegates.JobRequestArguments jobRequestArgs = new JobRequestArgumentsImpl()
        AgentJobRequest jobRequest

        when:
        jobRequest = converter.agentJobRequestArgsToDTO(jobRequestArgs)

        then:
        jobRequest.getTimeout() == Optional.ofNullable(jobRequestArgs.getTimeout())
        jobRequest.getCommandArgs() == jobRequestArgs.getCommandArguments()
        jobRequest.isArchivingDisabled() == jobRequestArgs.isArchivalDisabled()
        jobRequest.isInteractive() == jobRequestArgs.isInteractive()
        jobRequest.getRequestedJobDirectoryLocation() == Optional.ofNullable(jobRequestArgs.getJobDirectoryLocation())
        jobRequest.getCriteria() != null
        jobRequest.getCriteria().getApplicationIds() == jobRequestArgs.getApplicationIds()
        jobRequest.getCriteria().getClusterCriteria() == jobRequestArgs.getClusterCriteria()
        jobRequest.getCriteria().getCommandCriterion() == jobRequestArgs.getCommandCriterion()
        jobRequest.getMetadata().getEmail() == Optional.ofNullable(jobRequestArgs.getEmail())
        jobRequest.getMetadata().getGrouping() == Optional.ofNullable(jobRequestArgs.getGrouping())
        jobRequest.getMetadata().getGroupingInstance() == Optional.ofNullable(jobRequestArgs.getGroupingInstance())
        jobRequest.getMetadata().getDescription() == Optional.ofNullable(jobRequestArgs.getJobDescription())
        jobRequest.getMetadata().getTags() == jobRequestArgs.getJobTags()
        jobRequest.getMetadata().getVersion() == Optional.ofNullable(jobRequestArgs.getJobVersion())
        jobRequest.getMetadata().getName() == jobRequestArgs.getJobName()
        jobRequest.getMetadata().getMetadata() == Optional.ofNullable(jobRequestArgs.getJobMetadata())
        jobRequest.getMetadata().getGroup() == Optional.ofNullable(null)
        jobRequest.getMetadata().getUser() == System.getProperty('user.name')
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
        jobRequestArgs.getTimeout() >> 10
        jobRequest.getTimeout().get() == 10
        jobRequestArgs.getCommandArguments() >> ["foo", "bar"].asList()
        jobRequest.getCommandArgs() == ["foo", "bar"].asList()
        jobRequestArgs.isArchivalDisabled() >> true
        jobRequest.isArchivingDisabled()
        jobRequestArgs.isInteractive() >> true
        jobRequest.isInteractive()
        jobRequestArgs.getJobDirectoryLocation() >> new File("/tmp")
        jobRequest.getRequestedJobDirectoryLocation() == Optional.of(new File("/tmp"))
        jobRequestArgs.getClusterCriteria() >> [clusterCriterion1, clusterCriterion2].asList()
        jobRequest.getCriteria().getClusterCriteria() == [clusterCriterion1, clusterCriterion2].asList()
        jobRequestArgs.getCommandCriterion() >> commandCriterion
        jobRequest.getCriteria().getCommandCriterion() == commandCriterion
        jobRequestArgs.getApplicationIds() >> ["app1, app2"].asList()
        jobRequest.getCriteria().getApplicationIds() == ["app1, app2"].asList()
        jobRequestArgs.getEmail() >> "foo@bar.com"
        jobRequest.getMetadata().getEmail() == Optional.of("foo@bar.com")
        jobRequestArgs.getGrouping() >> "g"
        jobRequest.getMetadata().getGrouping() == Optional.of("g")
        jobRequestArgs.getGroupingInstance() >> "gi"
        jobRequest.getMetadata().getGroupingInstance() == Optional.of("gi")
        jobRequestArgs.getJobDescription() >> "d"
        jobRequest.getMetadata().getDescription() == Optional.of("d")
        jobRequestArgs.getJobTags() >> Sets.newHashSet(["t1", "t2"])
        jobRequest.getMetadata().getTags() == Sets.newHashSet(["t1", "t2"])
        jobRequestArgs.getJobVersion() >> "1.2.3"
        jobRequest.getMetadata().getVersion() == Optional.of("1.2.3")
        jobRequestArgs.getJobName() >> "n"
        jobRequest.getMetadata().getName() == "n"
        jobRequestArgs.getJobMetadata() >> metadata
        jobRequest.getMetadata().getMetadata() == Optional.of(metadata)
        jobRequestArgs.getUser() >> "u"
        jobRequest.getMetadata().getUser() == "u"
        validator.validate(_ as AgentJobRequest) >> Sets.newHashSet()
    }

    def "Convert fail validation"() {

        setup:
        ArgumentDelegates.JobRequestArguments jobRequestArgs = new JobRequestArgumentsImpl()
        ConstraintViolation<AgentJobRequest> violation = Mock()

        when:
        converter.agentJobRequestArgsToDTO(jobRequestArgs)

        then:
        validator.validate(_ as AgentJobRequest) >> Sets.newHashSet([violation])
        violation.getMessage() >> "NO!"
        thrown(JobRequestConverter.ConversionException)
    }
}
