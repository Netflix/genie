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
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.exceptions.checked.AgentLaunchException
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
        def executable = Lists.newArrayList("java", "-jar", "genie-agent.jar")
        def properties = Mock(LocalAgentLauncherProperties)
        def factory = Mock(ExecutorFactory)
        def rpcPort = 9090
        def registry = Mock(MeterRegistry)

        def jobId = UUID.randomUUID().toString()
        def jobSpecification = Mock(JobSpecification) {
            getJob() >> Mock(JobSpecification.ExecutionResource) {
                getId() >> jobId
            }
        }
        def jobEnvironment = Mock(JobEnvironment)
        def resolvedJob = new ResolvedJob(jobSpecification, jobEnvironment)
        def executor = Mock(Executor)

        def expectedLinuxExecutable = "setsid"
        String[] expectedLinuxArguments = [
            "java",
            "-jar",
            "genie-agent.jar",
            "exec",
            "--serverHost",
            "localhost",
            "--serverPort",
            Integer.toString(rpcPort),
            "--full-cleanup",
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
            "localhost",
            "--serverPort",
            Integer.toString(rpcPort),
            "--full-cleanup",
            "--api-job",
            "--jobId",
            jobId
        ]

        when:
        def launcher = new LocalAgentLauncherImpl(properties, rpcPort, factory, registry)

        then:
        1 * properties.getExecutable() >> executable

        when:
        launcher.launchAgent(resolvedJob)

        then:
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

        when:
        launcher.launchAgent(resolvedJob)

        then:
        1 * factory.newInstance(true) >> executor
        1 * executor.execute(_ as CommandLine, _ as ExecuteResultHandler) >> {
            throw new IOException("Something broke")
        }
        thrown(AgentLaunchException)
    }
}
