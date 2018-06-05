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

import com.beust.jcommander.JCommander
import com.beust.jcommander.ParametersDelegate
import com.netflix.genie.common.internal.dto.v4.Criterion
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class JobRequestArgumentsImplSpec extends Specification {

    TestOptions options
    JCommander jCommander

    void setup() {
        options = new TestOptions()
        jCommander = new JCommander(options)
    }

    void cleanup() {
    }

    def "Defaults"() {
        when:
        jCommander.parse()

        then:
        options.jobRequestArguments.getCommandArguments().isEmpty()
        options.jobRequestArguments.getJobDirectoryLocation() == new File(JobRequestArgumentsImpl.DEFAULT_JOBS_DIRECTORY)
        !options.jobRequestArguments.isInteractive()
        !options.jobRequestArguments.isArchivalDisabled()
        options.jobRequestArguments.getTimeout() == null
        options.jobRequestArguments.getJobId() == null
        options.jobRequestArguments.getClusterCriteria().isEmpty()
        options.jobRequestArguments.getCommandCriterion() == null
        options.jobRequestArguments.getApplicationIds().isEmpty()
        options.jobRequestArguments.getJobName() == null
        options.jobRequestArguments.getUser() == System.getProperty("user.name")
        options.jobRequestArguments.getEmail() == null
        options.jobRequestArguments.getGrouping() == null
        options.jobRequestArguments.getGroupingInstance() == null
        options.jobRequestArguments.getJobDescription() == null
        options.jobRequestArguments.getJobTags().isEmpty()
        options.jobRequestArguments.getJobVersion() == null
        options.jobRequestArguments.getJobMetadata() == GenieObjectMapper.getMapper().createObjectNode()
        !options.jobRequestArguments.isJobRequestedViaAPI()
    }

    def "Parse"() {
        when:
        jCommander.parse(
                "--jobDirectoryLocation", "/foo/bar",
                "--interactive",
                "--archivalDisabled",
                "--timeout", "10",
                "--jobId", "FooBar",
                "--clusterCriterion", "NAME=test",
                "--clusterCriterion", "NAME=prod",
                "--commandCriterion", "STATUS=active",
                "--applicationIds", "app1",
                "--applicationIds", "app2",
                "--jobName", "n",
                "--email", "e",
                "--grouping", "g",
                "--groupingInstance", "gi",
                "--jobDescription", "jd",
                "--jobTag", "t1",
                "--jobTag", "t2",
                "--jobVersion", "1.0",
                "--jobMetadata", "{\"foo\": false}",
                "--api-job",
                "foo", "bar",
        )

        then:
        options.jobRequestArguments.getJobDirectoryLocation() == new File("/foo/bar")
        options.jobRequestArguments.isInteractive()
        options.jobRequestArguments.isArchivalDisabled()
        options.jobRequestArguments.getTimeout() == 10
        options.jobRequestArguments.getJobId() == "FooBar"
        options.jobRequestArguments.getClusterCriteria().size() == 2
        options.jobRequestArguments.getClusterCriteria().containsAll([
                new Criterion.Builder().withName("prod").build(),
                new Criterion.Builder().withName("test").build(),
        ])
        options.jobRequestArguments.getCommandCriterion() == new Criterion.Builder().withStatus("active").build()
        options.jobRequestArguments.getApplicationIds().size() == 2
        options.jobRequestArguments.getApplicationIds().containsAll(["app1", "app2"])
        options.jobRequestArguments.getJobName() == "n"
        options.jobRequestArguments.getUser() == System.getProperty("user.name")
        options.jobRequestArguments.getEmail() == "e"
        options.jobRequestArguments.getGrouping() == "g"
        options.jobRequestArguments.getGroupingInstance() == "gi"
        options.jobRequestArguments.getJobDescription() == "jd"
        options.jobRequestArguments.getJobTags().size() == 2
        options.jobRequestArguments.getJobTags().containsAll(["t1", "t2"])
        options.jobRequestArguments.getJobVersion() == "1.0"
        options.jobRequestArguments.getJobMetadata() == GenieObjectMapper.getMapper().createObjectNode().put("foo", false)
        options.jobRequestArguments.getCommandArguments() == ["foo", "bar"].asList()
        options.jobRequestArguments.isJobRequestedViaAPI()
    }

    class TestOptions {
        @ParametersDelegate
        private ArgumentDelegates.JobRequestArguments jobRequestArguments = new JobRequestArgumentsImpl()
    }
}
