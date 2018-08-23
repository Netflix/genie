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
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class AgentOptionsArgumentsImplSpec extends Specification {

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
        options.agentOptionsArguments.getJobFolderCleanUpOption() == JobFolderCleanupOption.DELETE_DEPENDENCIES_ONLY
    }

    def "Parse Full clean up"() {
        when:
        jCommander.parse("--full-cleanup")

        then:
        options.agentOptionsArguments.getJobFolderCleanUpOption() == JobFolderCleanupOption.DELETE_JOB_FOLDER
    }

    def "Parse No clean up"() {
        when:
        jCommander.parse("--no-cleanup")

        then:
        options.agentOptionsArguments.getJobFolderCleanUpOption() == JobFolderCleanupOption.NO_CLEANUP
    }

    class TestOptions {
        @ParametersDelegate
        private ArgumentDelegates.AgentOptions agentOptionsArguments = new AgentOptionsArgumentsImpl()
    }
}
