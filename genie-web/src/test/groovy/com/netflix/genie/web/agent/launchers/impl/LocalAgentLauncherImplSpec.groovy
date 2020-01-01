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

import com.netflix.genie.common.external.dtos.v4.JobEnvironment
import com.netflix.genie.common.external.dtos.v4.JobMetadata
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.web.data.services.JobSearchService
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.introspection.GenieWebHostInfo
import com.netflix.genie.web.introspection.GenieWebRpcInfo
import com.netflix.genie.web.properties.LocalAgentLauncherProperties
import com.netflix.genie.web.util.ExecutorFactory
import io.micrometer.core.instrument.MeterRegistry
import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.Executor
import org.apache.commons.lang3.SystemUtils
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link LocalAgentLauncherImpl}.
 *
 * @author tgianos
 */
class LocalAgentLauncherImplSpec extends Specification {

    static final int RPC_PORT = new Random().nextInt()
    static final String JOB_ID = UUID.randomUUID().toString()
    static final String USERNAME = UUID.randomUUID().toString()
    static final List<String> expectedCommandLineBase = [
        "java", "-jar", "/tmp/genie-agent.jar",
        "exec",
        "--server-host", "127.0.0.1",
        "--server-port", String.valueOf(RPC_PORT),
        "--api-job",
        "--job-id", JOB_ID
    ] as List<String>

    GenieWebHostInfo hostInfo
    GenieWebRpcInfo rpcInfo
    JobSearchService jobSearchService
    LocalAgentLauncherProperties launchProperties
    ExecutorFactory executorFactory
    MeterRegistry meterRegistry

    LocalAgentLauncherImpl launcher
    String hostname
    ResolvedJob resolvedJob
    JobMetadata jobMetadata
    JobEnvironment jobEnvironment
    int jobMemory
    JobSpecification jobSpec
    JobSpecification.ExecutionResource job
    Executor sharedExecutor
    Executor executor
    Map<String, String> additionalEnvironment

    def setup() {
        this.hostInfo = Mock(GenieWebHostInfo)
        this.rpcInfo = Mock(GenieWebRpcInfo)
        this.jobSearchService = Mock(JobSearchService)
        this.launchProperties = new LocalAgentLauncherProperties()
        this.executorFactory = Mock(ExecutorFactory)
        this.meterRegistry = Mock(MeterRegistry)

        this.sharedExecutor = Mock(Executor)
        this.hostname = UUID.randomUUID().toString()
        this.resolvedJob = Mock(ResolvedJob)
        this.jobMetadata = Mock(JobMetadata)
        this.jobEnvironment = Mock(JobEnvironment)
        this.jobMemory = 100
        this.jobSpec = Mock(JobSpecification)
        this.job = Mock(JobSpecification.ExecutionResource)
        this.executor = Mock(Executor)
        this.additionalEnvironment = [foo: "bar"]
    }

    @Unroll
    def "Launch agent (runAsUser: #runAsUser)"(boolean runAsUser, List<String> expectedCommandLine) {
        this.launchProperties.setRunAsUserEnabled(runAsUser)
        this.launchProperties.setAdditionalEnvironment(additionalEnvironment)

        expectedCommandLine = (SystemUtils.IS_OS_LINUX ? ["setsid"] : []) + expectedCommandLine

        when:
        this.launcher = new LocalAgentLauncherImpl(
            hostInfo,
            rpcInfo,
            jobSearchService,
            launchProperties,
            executorFactory,
            meterRegistry
        )

        then:
        1 * this.hostInfo.getHostname() >> this.hostname
        1 * this.rpcInfo.getRpcPort() >> RPC_PORT
        1 * this.executorFactory.newInstance(false) >> this.sharedExecutor

        when:
        this.launcher.launchAgent(this.resolvedJob)

        then:
        1 * this.resolvedJob.getJobMetadata() >> this.jobMetadata
        1 * this.jobMetadata.getUser() >> USERNAME
        if (runAsUser) {
            1 * this.jobMetadata.getGroup() >> Optional.empty()
        } else {
            0 * this.jobMetadata.getGroup() >> Optional.empty()
        }

        1 * this.resolvedJob.getJobEnvironment() >> this.jobEnvironment
        1 * this.jobEnvironment.getMemory() >> this.jobMemory
        1 * this.resolvedJob.getJobSpecification() >> this.jobSpec
        1 * this.jobSpec.getJob() >> this.job
        1 * this.job.getId() >> JOB_ID
        1 * this.jobSearchService.getUsedMemoryOnHost(this.hostname)
        1 * this.executorFactory.newInstance(true) >> executor
        1 * this.executor.execute(_ as CommandLine, _ as Map, _ as LocalAgentLauncherImpl.AgentResultHandler) >> {
            args ->
                CommandLine commandLine = args[0] as CommandLine
                Map<String, String> env = args[1] as Map<String, String>
                assert expectedCommandLine.toString() == commandLine.toString()
                assert env.get("foo") == "bar"
                assert env.size() > 1
        }

        where:
        runAsUser | expectedCommandLine
        false     | expectedCommandLineBase
        true      | ["sudo", "-E", "-u", USERNAME] + expectedCommandLineBase
    }
}
