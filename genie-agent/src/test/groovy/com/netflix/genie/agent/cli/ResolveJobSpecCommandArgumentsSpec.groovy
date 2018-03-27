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
import com.beust.jcommander.ParameterException
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class ResolveJobSpecCommandArgumentsSpec extends Specification {

    ResolveJobSpecCommand.ResolveJobSpecCommandArguments options
    JCommander jCommander
    ArgumentDelegates.ServerArguments serverArguments
    ArgumentDelegates.JobRequestArguments jobArguments

    void setup() {
        serverArguments = new ServerArgumentsImpl()
        jobArguments = new JobRequestArgumentsImpl()
        options = new ResolveJobSpecCommand.ResolveJobSpecCommandArguments(serverArguments, jobArguments)
        jCommander = new JCommander(options)
    }

    void cleanup() {
    }

    def "Defaults"() {
        when:
        jCommander.parse()

        then:
        options.getSpecificationId() == null
        options.getOutputFile() == null
        !options.isPrintRequestDisabled()
        options.getJobRequestArguments() != null
        options.getServerArguments() != null
    }

    def "Parse"() {
        when:
        jCommander.parse(
                "--serverHost", "server.com",
                "--serverPort", "1234",
                "--spec-id", "666666",
                "--no-request",
                "--output-file", "/foo/spec.json",
                "foo", "bar"
        )

        then:
        "server.com" == options.getServerArguments().getServerHost()
        1234 == options.getServerArguments().getServerPort()
        0 == options.getJobRequestArguments().getClusterCriteria().size()
        "666666" == options.getSpecificationId()
        options.isPrintRequestDisabled()
        new File("/foo/spec.json") == options.getOutputFile()
        ["foo", "bar"].asList() == options.getJobRequestArguments().getCommandArguments()
    }

    def "InvalidRequestId"() {
        when:
        jCommander.parse(
                "--commandCriterion", "/",
        )

        then:
        thrown(ParameterException)
    }
}
