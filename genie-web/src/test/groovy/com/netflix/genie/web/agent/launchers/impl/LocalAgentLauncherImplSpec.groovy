/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.agent.launchers.impl

import com.google.common.collect.Lists
import com.netflix.genie.common.internal.dto.v4.JobEnvironment
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.web.data.services.JobSearchService
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.exceptions.checked.AgentLaunchException
import com.netflix.genie.web.introspection.GenieWebHostInfo
import com.netflix.genie.web.introspection.GenieWebRpcInfo
import com.netflix.genie.web.properties.LocalAgentLauncherProperties
import com.netflix.genie.web.util.ExecutorFactory
import io.micrometer.core.instrument.MeterRegistry
import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.ExecuteResultHandler
import org.apache.commons.exec.Executor
import org.apache.commons.lang3.SystemUtils
import spock.lang.Specification

/**
 * Specifications for {@link LocalAgentLauncherImpl}.
 *
 * @author tgianos
 */
class LocalAgentLauncherImplSpec extends Specification {

    def "Can Launch Agent"() {
        def hostname = UUID.randomUUID().toString()
        def rpcPort = 9090
        def genieHostInfo = Mock(GenieWebHostInfo) {
            getHostname() >> hostname
        }
        def genieRpcInfo = Mock(GenieWebRpcInfo) {
            getRpcPort() >> rpcPort
        }
        def jobSearchService = Mock(JobSearchService)
        def executable = Lists.newArrayList("java", "-jar", "genie-agent.jar")
        def properties = Mock(LocalAgentLauncherProperties)
        def factory = Mock(ExecutorFactory)
        def registry = Mock(MeterRegistry)

        def jobId = UUID.randomUUID().toString()
        def jobMemory = 10_240
        def jobSpecification = Mock(JobSpecification) {
            getJob() >> Mock(JobSpecification.ExecutionResource) {
                getId() >> jobId
            }
        }
        def jobEnvironment = Mock(JobEnvironment) {
            getMemory() >> jobMemory
        }
        def resolvedJob = new ResolvedJob(jobSpecification, jobEnvironment)
        def executor = Mock(Executor)

        def expectedLinuxExecutable = "setsid"
        String[] expectedLinuxArguments = [
            "java",
            "-jar",
            "genie-agent.jar",
            "exec",
            "--serverHost",
            "127.0.0.1",
            "--serverPort",
            Integer.toString(rpcPort),
            "--no-cleanup",
            "--api-job",
            "--jobId",
            jobId
        ]

        def expectedNonLinuxExecutable = "java"
        String[] expectedNonLinuxArguments = [
            "-jar",
            "genie-agent.jar",
            "exec",
            "--serverHost",
            "127.0.0.1",
            "--serverPort",
            Integer.toString(rpcPort),
            "--no-cleanup",
            "--api-job",
            "--jobId",
            jobId
        ]

        when:
        def launcher = new LocalAgentLauncherImpl(
            genieHostInfo,
            genieRpcInfo,
            jobSearchService,
            properties,
            factory,
            registry
        )

        then:
        1 * properties.getExecutable() >> executable

        when: "A resolved job passes all checks"
        launcher.launchAgent(resolvedJob)

        then: "An agent is successfully launched"
        1 * properties.getMaxJobMemory() >> jobMemory + 1
        1 * jobSearchService.getUsedMemoryOnHost(hostname) >> 0
        1 * properties.getMaxTotalJobMemory() >> jobMemory + 1
        1 * factory.newInstance(true) >> executor
        1 * executor.execute(
            {
                CommandLine commandLine ->
                    if (SystemUtils.IS_OS_LINUX) {
                        assert commandLine.getExecutable() == expectedLinuxExecutable
                        assert commandLine.getArguments() == expectedLinuxArguments
                    } else {
                        assert commandLine.getExecutable() == expectedNonLinuxExecutable
                        assert commandLine.getArguments() == expectedNonLinuxArguments
                    }
            },
            {
                ExecuteResultHandler handler ->
                    assert handler != null
                    assert handler instanceof LocalAgentLauncherImpl.AgentResultHandler
            }
        )

        when: "The job requests more memory than the system is configured to allow for a max per job"
        launcher.launchAgent(resolvedJob)

        then: "An AgentLaunchException is thrown"
        2 * properties.getMaxJobMemory() >> jobMemory - 1
        0 * jobSearchService.getUsedMemoryOnHost(hostname)
        0 * properties.getMaxTotalJobMemory()
        0 * factory.newInstance(true)
        0 * executor.execute(_ as CommandLine, _ as ExecuteResultHandler)
        thrown(AgentLaunchException)

        when: "Running the job would put the system over the max configured memory"
        launcher.launchAgent(resolvedJob)

        then: "An AgentLaunchException is thrown"
        1 * properties.getMaxJobMemory() >> jobMemory + 1
        1 * jobSearchService.getUsedMemoryOnHost(hostname) >> jobMemory
        2 * properties.getMaxTotalJobMemory() >> jobMemory
        0 * factory.newInstance(true)
        0 * executor.execute(_ as CommandLine, _ as ExecuteResultHandler)
        thrown(AgentLaunchException)

        when: "The command line can't be executed"
        launcher.launchAgent(resolvedJob)

        then: "An AgentLaunchException is thrown"
        1 * properties.getMaxJobMemory() >> jobMemory + 1
        1 * jobSearchService.getUsedMemoryOnHost(hostname) >> 0
        1 * properties.getMaxTotalJobMemory() >> jobMemory + 1
        1 * factory.newInstance(true) >> executor
        1 * executor.execute(_ as CommandLine, _ as ExecuteResultHandler) >> {
            throw new IOException("Something broke")
        }
        thrown(AgentLaunchException)
    }
}
