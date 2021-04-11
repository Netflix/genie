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

import brave.Span
import brave.Tracer
import brave.propagation.TraceContext
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.genie.common.external.dtos.v4.JobEnvironment
import com.netflix.genie.common.external.dtos.v4.JobMetadata
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.data.services.impl.jpa.queries.aggregates.JobInfoAggregate
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.introspection.GenieWebHostInfo
import com.netflix.genie.web.introspection.GenieWebRpcInfo
import com.netflix.genie.web.properties.LocalAgentLauncherProperties
import com.netflix.genie.web.util.ExecutorFactory
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.Executor
import org.apache.commons.lang3.SystemUtils
import org.springframework.boot.actuate.health.Status
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration

/**
 * Specifications for {@link LocalAgentLauncherImpl}.
 *
 * @author tgianos
 */
@SuppressWarnings("GroovyAccessibility")
class LocalAgentLauncherImplSpec extends Specification {

    static final int RPC_PORT = new Random().nextInt()
    static final String JOB_ID = UUID.randomUUID().toString()
    static final String USERNAME = UUID.randomUUID().toString()
    static final String HOSTNAME = "genie.netflix.net"
    static final List<String> expectedCommandLineBase = [
        "java", "-jar", "/tmp/genie-agent.jar",
        "exec",
        "--server-host", HOSTNAME,
        "--server-port", String.valueOf(RPC_PORT),
        "--api-job",
        "--job-id", JOB_ID
    ] as List<String>

    GenieWebHostInfo hostInfo
    GenieWebRpcInfo rpcInfo
    PersistenceService persistenceService
    LocalAgentLauncherProperties launchProperties
    ExecutorFactory executorFactory
    MeterRegistry meterRegistry
    DataServices dataServices

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
    JsonNode requestedLauncherExt
    Tracer tracer
    BraveTracePropagator tracePropagator
    BraveTracingComponents tracingComponents

    def setup() {
        this.hostname = UUID.randomUUID().toString()
        this.hostInfo = Mock(GenieWebHostInfo) {
            getHostname() >> this.hostname
        }
        this.rpcInfo = Mock(GenieWebRpcInfo) {
            getRpcPort() >> RPC_PORT
        }
        this.persistenceService = Mock(PersistenceService)
        this.launchProperties = new LocalAgentLauncherProperties()
        this.executorFactory = Mock(ExecutorFactory)
        this.meterRegistry = new SimpleMeterRegistry()
        this.sharedExecutor = Mock(Executor)
        this.resolvedJob = Mock(ResolvedJob)
        this.jobMetadata = Mock(JobMetadata)
        this.jobEnvironment = Mock(JobEnvironment)
        this.jobMemory = 100
        this.jobSpec = Mock(JobSpecification)
        this.job = Mock(JobSpecification.ExecutionResource)
        this.executor = Mock(Executor)
        this.additionalEnvironment = [foo: "bar"]
        this.dataServices = Mock(DataServices) {
            getPersistenceService() >> this.persistenceService
        }
        this.requestedLauncherExt = null
        this.launchProperties.setServerHostname(HOSTNAME)
        this.tracer = Mock(Tracer)
        this.tracePropagator = Mock(BraveTracePropagator)
        this.tracingComponents = new BraveTracingComponents(
            this.tracer,
            this.tracePropagator,
            Mock(BraveTracingCleanup),
            Mock(BraveTagAdapter)
        )
    }

    @Unroll
    def "Launch agent (runAsUser: #runAsUser)"(boolean runAsUser, List<String> expectedCommandLine) {
        this.launchProperties.setRunAsUserEnabled(runAsUser)
        this.launchProperties.setAdditionalEnvironment(this.additionalEnvironment)
        def jobInfo = Mock(JobInfoAggregate)
        def currentSpan = Mock(Span) {
            context() >> TraceContext.newBuilder()
                .traceId(UUID.randomUUID().getLeastSignificantBits())
                .traceIdHigh(UUID.randomUUID().getMostSignificantBits())
                .spanId(UUID.randomUUID().getLeastSignificantBits())
                .sampled(true)
                .build()
        }

        expectedCommandLine = (SystemUtils.IS_OS_LINUX ? ["setsid"] : []) + expectedCommandLine

        when:
        this.launcher = new LocalAgentLauncherImpl(
            this.hostInfo,
            this.rpcInfo,
            this.dataServices,
            this.launchProperties,
            this.executorFactory,
            this.tracingComponents,
            this.meterRegistry
        )

        then:
        1 * this.executorFactory.newInstance(false) >> this.sharedExecutor
        1 * this.persistenceService.getHostJobInformation(this.hostname) >> jobInfo
        1 * jobInfo.getNumberOfActiveJobs() >> 3L
        1 * jobInfo.getTotalMemoryAllocated() >> 4_000L
        0 * jobInfo.getTotalMemoryUsed()

        when:
        Optional<JsonNode> launcherExt = this.launcher.launchAgent(this.resolvedJob, this.requestedLauncherExt)

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
        1 * this.persistenceService.getUsedMemoryOnHost(this.hostname)
        1 * this.tracer.currentSpan() >> currentSpan
        1 * this.tracePropagator.injectForAgent(_ as TraceContext) >> new HashMap<>()
        1 * this.executorFactory.newInstance(true) >> this.executor
        1 * this.executor.execute(_ as CommandLine, _ as Map, _ as LocalAgentLauncherImpl.AgentResultHandler) >> {
            args ->
                CommandLine commandLine = args[0] as CommandLine
                Map<String, String> env = args[1] as Map<String, String>
                assert expectedCommandLine.toString() == commandLine.toString()
                assert env.get("foo") == "bar"
                assert env.size() > 1
        }
        launcherExt.isPresent()

        where:
        runAsUser | expectedCommandLine
        false     | expectedCommandLineBase
        true      | ["sudo", "-E", "-u", USERNAME] + expectedCommandLineBase
    }

    def "Host info calculation throwing error causes system to report down"() {
        def properties = Mock(LocalAgentLauncherProperties) {
            // Protect against test running a really long time
            getHostInfoExpireAfter() >> Duration.ofHours(2L)
            getHostInfoRefreshAfter() >> Duration.ofHours(1L)
        }

        when: "For some reason the host info is unable to be retrieved"
        def healthIndicator = new LocalAgentLauncherImpl(
            this.hostInfo,
            this.rpcInfo,
            this.dataServices,
            properties,
            this.executorFactory,
            this.tracingComponents,
            this.meterRegistry
        )
        def health = healthIndicator.health()

        then: "The system reports down"
        2 * this.persistenceService.getHostJobInformation(this.hostname) >> { throw new RuntimeException() }
        health.getStatus() == Status.DOWN
    }

    def "Host info is null the system reports unknown status"() {
        def properties = Mock(LocalAgentLauncherProperties) {
            // Protect against test running a really long time
            getHostInfoExpireAfter() >> Duration.ofHours(2L)
            getHostInfoRefreshAfter() >> Duration.ofHours(1L)
        }

        when: "For some reason the host info is null"
        def healthIndicator = new LocalAgentLauncherImpl(
            this.hostInfo,
            this.rpcInfo,
            this.dataServices,
            properties,
            this.executorFactory,
            this.tracingComponents,
            this.meterRegistry
        )
        def health = healthIndicator.health()

        then: "The system reports unknown state"
        2 * this.persistenceService.getHostJobInformation(this.hostname) >> null
        health.getStatus() == Status.UNKNOWN
        health.getDetails().size() == 1
    }

    def "Can report health"() {
        def jobInfo = Mock(JobInfoAggregate)
        def maxTotalJobMemory = 100_003L
        def maxJobMemory = 10_000
        def properties = Mock(LocalAgentLauncherProperties) {
            getMaxTotalJobMemory() >> maxTotalJobMemory
            getMaxJobMemory() >> maxJobMemory
            // Protect against test running a really long time
            getHostInfoExpireAfter() >> Duration.ofHours(2L)
            getHostInfoRefreshAfter() >> Duration.ofHours(1L)
        }

        when: "Available memory is equal to one max job"
        def healthIndicator = new LocalAgentLauncherImpl(
            this.hostInfo,
            this.rpcInfo,
            this.dataServices,
            properties,
            this.executorFactory,
            this.tracingComponents,
            this.meterRegistry
        )
        def health = healthIndicator.health()

        then: "The system reports healthy and the job info is cached"
        1 * this.persistenceService.getHostJobInformation(this.hostname) >> jobInfo
        2 * jobInfo.getTotalMemoryAllocated() >> maxTotalJobMemory - maxJobMemory
        1 * jobInfo.getTotalMemoryUsed() >> maxTotalJobMemory - 2 * maxJobMemory
        2 * jobInfo.getNumberOfActiveJobs() >> 335L
        health.getStatus() == Status.UP
        health.getDetails().get(LocalAgentLauncherImpl.NUMBER_ACTIVE_JOBS_KEY) == 335L
        health.getDetails().get(LocalAgentLauncherImpl.ALLOCATED_MEMORY_KEY) == maxTotalJobMemory - maxJobMemory
        health.getDetails().get(LocalAgentLauncherImpl.AVAILABLE_MEMORY_KEY) == maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory)
        health.getDetails().get(LocalAgentLauncherImpl.USED_MEMORY_KEY) == maxTotalJobMemory - 2 * maxJobMemory
        health.getDetails().get(LocalAgentLauncherImpl.AVAILABLE_MAX_JOB_CAPACITY_KEY) == ((maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory)) / maxJobMemory).toInteger()

        when: "Available memory is equal to more than one max job"
        health = healthIndicator.health()

        then: "Cached job information is used and the system reports healthy"
        1 * jobInfo.getTotalMemoryAllocated() >> maxTotalJobMemory - maxJobMemory - 1
        1 * jobInfo.getTotalMemoryUsed() >> maxTotalJobMemory - 2 * maxJobMemory
        1 * jobInfo.getNumberOfActiveJobs() >> 337L
        health.getStatus() == Status.UP
        health.getDetails().get(LocalAgentLauncherImpl.NUMBER_ACTIVE_JOBS_KEY) == 337L
        health.getDetails().get(LocalAgentLauncherImpl.ALLOCATED_MEMORY_KEY) == maxTotalJobMemory - maxJobMemory - 1
        health.getDetails().get(LocalAgentLauncherImpl.AVAILABLE_MEMORY_KEY) == maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory - 1)
        health.getDetails().get(LocalAgentLauncherImpl.USED_MEMORY_KEY) == maxTotalJobMemory - 2 * maxJobMemory
        health.getDetails().get(LocalAgentLauncherImpl.AVAILABLE_MAX_JOB_CAPACITY_KEY) == ((maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory - 1)) / maxJobMemory).toInteger()

        when: "Available memory is less than one max job"
        health = healthIndicator.health()

        then: "Cached job information is used and the system reports down"
        1 * jobInfo.getTotalMemoryAllocated() >> maxTotalJobMemory - maxJobMemory + 1
        1 * jobInfo.getTotalMemoryUsed() >> maxTotalJobMemory - 2 * maxJobMemory
        1 * jobInfo.getNumberOfActiveJobs() >> 343L
        health.getStatus() == Status.DOWN
        health.getDetails().get(LocalAgentLauncherImpl.NUMBER_ACTIVE_JOBS_KEY) == 343L
        health.getDetails().get(LocalAgentLauncherImpl.ALLOCATED_MEMORY_KEY) == maxTotalJobMemory - maxJobMemory + 1
        health.getDetails().get(LocalAgentLauncherImpl.AVAILABLE_MEMORY_KEY) == maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory + 1)
        health.getDetails().get(LocalAgentLauncherImpl.USED_MEMORY_KEY) == maxTotalJobMemory - 2 * maxJobMemory
        health.getDetails().get(LocalAgentLauncherImpl.AVAILABLE_MAX_JOB_CAPACITY_KEY) == ((maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory + 1)) / maxJobMemory).toInteger()
    }
}
