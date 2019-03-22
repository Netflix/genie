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
package com.netflix.genie.common.internal.dto.v4

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.test.suppliers.RandomSuppliers
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link JobRequest} class.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
class JobRequestSpec extends Specification {

    def "Can build immutable api job request"() {
        def metadata = new JobMetadata.Builder(UUID.randomUUID().toString(), UUID.randomUUID().toString()).build()
        def criteria = new ExecutionResourceCriteria(
            Lists.newArrayList(new Criterion.Builder().withId(UUID.randomUUID().toString()).build()),
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(),
            null
        )
        def requestedId = UUID.randomUUID().toString()
        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def timeout = 180
        def interactive = true
        def archivingDisabled = true
        def requestedArchiveLocationPrefix = UUID.randomUUID().toString()
        def jobResources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def jobDirectoryLocation = "/tmp"
        def requestedEnvironmentVariables = ImmutableMap.of(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def requestedAgentEnvironment = new AgentEnvironmentRequest.Builder()
            .withRequestedJobCpu(3)
            .withRequestedJobMemory(10_000)
            .withRequestedEnvironmentVariables(requestedEnvironmentVariables)
            .withExt(GenieObjectMapper.getMapper().readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
            .build()
        def requestedAgentConfig = new AgentConfigRequest.Builder()
            .withArchivingDisabled(archivingDisabled)
            .withTimeoutRequested(timeout)
            .withInteractive(interactive)
            .withRequestedJobDirectoryLocation(jobDirectoryLocation)
            .withExt(GenieObjectMapper.getMapper().readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
            .build()

        def requestedJobArchivalData = new JobArchivalDataRequest.Builder()
            .withRequestedArchiveLocationPrefix(requestedArchiveLocationPrefix)
            .build()

        ApiJobRequest jobRequest

        when:
        jobRequest = new ApiJobRequest.Builder(metadata, criteria)
            .withRequestedId(requestedId)
            .withCommandArgs(commandArgs)
            .withResources(jobResources)
            .withRequestedAgentEnvironment(requestedAgentEnvironment)
            .withRequestedAgentConfig(requestedAgentConfig)
            .withRequestedJobArchivalData(requestedJobArchivalData)
            .build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        jobRequest.getRequestedId().orElse(UUID.randomUUID().toString()) == requestedId
        jobRequest.getCommandArgs() == commandArgs
        jobRequest.getRequestedAgentConfig() == requestedAgentConfig
        jobRequest.getResources() == jobResources
        jobRequest.getRequestedAgentEnvironment() == requestedAgentEnvironment
        jobRequest.getRequestedJobArchivalData() == requestedJobArchivalData

        when:
        jobRequest = new ApiJobRequest.Builder(metadata, criteria).build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        jobRequest.getResources() == new ExecutionEnvironment(null, null, null)
        jobRequest.getRequestedAgentEnvironment() == new AgentEnvironmentRequest.Builder().build()
        jobRequest.getRequestedAgentConfig() == new AgentConfigRequest.Builder().build()
        jobRequest.getRequestedJobArchivalData() == new JobArchivalDataRequest.Builder().build()

        when:
        jobRequest = new ApiJobRequest.Builder(metadata, criteria)
            .withCommandArgs(null)
            .withRequestedAgentEnvironment(null)
            .withRequestedAgentConfig(null)
            .withRequestedJobArchivalData(null)
            .build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        jobRequest.getResources() == new ExecutionEnvironment(null, null, null)
        jobRequest.getRequestedAgentEnvironment() == new AgentEnvironmentRequest.Builder().build()
        jobRequest.getRequestedAgentConfig() == new AgentConfigRequest.Builder().build()
        jobRequest.getRequestedJobArchivalData() == new JobArchivalDataRequest.Builder().build()

        when: "Command args are blank they're ignored"
        jobRequest = new ApiJobRequest.Builder(metadata, criteria)
            .withCommandArgs(Lists.newArrayList(" ", "\t"))
            .withRequestedAgentEnvironment(null)
            .withRequestedAgentConfig(null)
            .withRequestedJobArchivalData(null)
            .build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        jobRequest.getResources() == new ExecutionEnvironment(null, null, null)
        jobRequest.getRequestedAgentEnvironment() == new AgentEnvironmentRequest.Builder().build()
        jobRequest.getRequestedAgentConfig() == new AgentConfigRequest.Builder().build()
        jobRequest.getRequestedJobArchivalData() == new JobArchivalDataRequest.Builder().build()
    }

    def "Can build immutable agent job request"() {
        def metadata = new JobMetadata.Builder(UUID.randomUUID().toString(), UUID.randomUUID().toString()).build()
        def criteria = new ExecutionResourceCriteria(
            Lists.newArrayList(new Criterion.Builder().withId(UUID.randomUUID().toString()).build()),
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(),
            null
        )
        def requestedId = UUID.randomUUID().toString()
        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def timeout = 180
        def interactive = true
        def archivingDisabled = true
        def requestedArchiveLocationPrefix = UUID.randomUUID().toString()
        def jobResources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def jobDirectoryLocation = "/tmp"
        def requestedAgentConfig = new AgentConfigRequest.Builder()
            .withArchivingDisabled(archivingDisabled)
            .withTimeoutRequested(timeout)
            .withInteractive(interactive)
            .withRequestedJobDirectoryLocation(jobDirectoryLocation)
            .withExt(GenieObjectMapper.getMapper().readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
            .build()
        def requestedJobArchivalData = new JobArchivalDataRequest.Builder()
            .withRequestedArchiveLocationPrefix(requestedArchiveLocationPrefix)
            .build()
        AgentJobRequest jobRequest

        when:
        jobRequest = new AgentJobRequest.Builder(metadata, criteria, requestedAgentConfig, requestedJobArchivalData)
            .withRequestedId(requestedId)
            .withCommandArgs(commandArgs)
            .withResources(jobResources)
            .build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        jobRequest.getRequestedId().orElse(UUID.randomUUID().toString()) == requestedId
        jobRequest.getCommandArgs() == commandArgs
        jobRequest.getRequestedAgentConfig() == requestedAgentConfig
        jobRequest.getResources() == jobResources
        jobRequest.getRequestedJobArchivalData() == requestedJobArchivalData

        when:
        jobRequest = new AgentJobRequest.Builder(metadata, criteria, requestedAgentConfig, requestedJobArchivalData).build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        jobRequest.getRequestedAgentConfig() == requestedAgentConfig
        jobRequest.getResources() != null
        jobRequest.requestedJobArchivalData == requestedJobArchivalData

        when:
        jobRequest = new AgentJobRequest.Builder(metadata, criteria, requestedAgentConfig, requestedJobArchivalData)
            .withCommandArgs(null)
            .build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        jobRequest.getRequestedAgentConfig() == requestedAgentConfig
        jobRequest.getResources() != null
        jobRequest.requestedJobArchivalData == requestedJobArchivalData


        when: "Empty command args are supplied they're ignored"
        jobRequest = new AgentJobRequest.Builder(metadata, criteria, requestedAgentConfig, requestedJobArchivalData)
            .withCommandArgs(Lists.newArrayList(" ", "\n"))
            .build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        jobRequest.getRequestedAgentConfig() == requestedAgentConfig
        jobRequest.getResources() != null
        jobRequest.requestedJobArchivalData == requestedJobArchivalData

        when: "Can build with missing job archival data"
        jobRequest = new AgentJobRequest.Builder(metadata, criteria, requestedAgentConfig, null)
            .withCommandArgs(Lists.newArrayList(" ", "\n"))
            .build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        jobRequest.getRequestedAgentConfig() == requestedAgentConfig
        jobRequest.getResources() != null
        jobRequest.requestedJobArchivalData == new JobArchivalDataRequest.Builder().build()

    }

    def "Can build job request"() {
        def metadata = new JobMetadata.Builder(UUID.randomUUID().toString(), UUID.randomUUID().toString()).build()
        def criteria = new ExecutionResourceCriteria(
            Lists.newArrayList(new Criterion.Builder().withId(UUID.randomUUID().toString()).build()),
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(),
            null
        )
        def requestedId = UUID.randomUUID().toString()
        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def timeout = 180
        def interactive = true
        def archivingDisabled = true
        def requestedArchiveLocationPrefix = UUID.randomUUID().toString()
        def jobResources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def jobDirectoryLocation = "/tmp"
        def requestedEnvironmentVariables = ImmutableMap.of(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def requestedAgentEnvironment = new AgentEnvironmentRequest.Builder()
            .withRequestedEnvironmentVariables(requestedEnvironmentVariables)
            .withRequestedJobCpu(2)
            .withRequestedJobMemory(10_000)
            .withExt(GenieObjectMapper.getMapper().readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
            .build()
        def requestedAgentConfig = new AgentConfigRequest.Builder()
            .withArchivingDisabled(archivingDisabled)
            .withTimeoutRequested(timeout)
            .withInteractive(interactive)
            .withRequestedJobDirectoryLocation(jobDirectoryLocation)
            .withExt(GenieObjectMapper.getMapper().readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
            .build()

        def requestedJobArchivalData = new JobArchivalDataRequest.Builder()
            .withRequestedArchiveLocationPrefix(requestedArchiveLocationPrefix)
            .build()

        JobRequest jobRequest

        when:
        jobRequest = new JobRequest(
            requestedId,
            jobResources,
            commandArgs,
            metadata,
            criteria,
            requestedAgentEnvironment,
            requestedAgentConfig,
            requestedJobArchivalData
        )

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        jobRequest.getRequestedId().orElse(UUID.randomUUID().toString()) == requestedId
        jobRequest.getCommandArgs() == commandArgs
        jobRequest.getRequestedAgentConfig() == requestedAgentConfig
        jobRequest.getResources() == jobResources
        jobRequest.getRequestedAgentEnvironment() == requestedAgentEnvironment
        jobRequest.getRequestedJobArchivalData() == requestedJobArchivalData

        when:
        jobRequest = new JobRequest(
            null,
            null,
            null,
            metadata,
            criteria,
            null,
            null,
            null
        )

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        jobRequest.getResources() == new ExecutionEnvironment(null, null, null)
        jobRequest.getRequestedAgentEnvironment() == new AgentEnvironmentRequest.Builder().build()
        jobRequest.getRequestedAgentConfig() == new AgentConfigRequest.Builder().build()
        jobRequest.getRequestedJobArchivalData() == new JobArchivalDataRequest.Builder().build()

        when:
        "Empty command args are supplied they're ignored"
        jobRequest = new JobRequest(
            null,
            null,
            Lists.newArrayList("\n\n"),
            metadata,
            criteria,
            null,
            null,
            null
        )

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        jobRequest.getResources() == new ExecutionEnvironment(null, null, null)
        jobRequest.getRequestedAgentEnvironment() == new AgentEnvironmentRequest.Builder().build()
        jobRequest.getRequestedAgentConfig() == new AgentConfigRequest.Builder().build()
        jobRequest.getRequestedJobArchivalData() == new JobArchivalDataRequest.Builder().build()
    }

    def "Test equals"() {
        def base = createJobRequest()
        Object comparable

        when:
        comparable = base

        then:
        base == comparable

        when:
        comparable = null

        then:
        base != comparable

        when:
        comparable = createJobRequest()

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def jobMetadata = Mock(JobMetadata)
        def criteria = Mock(ExecutionResourceCriteria)
        def agentEnvironmentRequest = new AgentEnvironmentRequest.Builder().build()
        def agentConfigRequest = new AgentConfigRequest.Builder().build()
        def jobArchivalDataRequest = new JobArchivalDataRequest.Builder().build()
        base = new JobRequest(null, null, null, jobMetadata, criteria, agentEnvironmentRequest, agentConfigRequest, jobArchivalDataRequest)
        comparable = new JobRequest(null, null, null, jobMetadata, criteria, agentEnvironmentRequest, agentConfigRequest, jobArchivalDataRequest)

        then:
        base == comparable
    }

    def "Test hashCode"() {
        JobRequest one
        JobRequest two

        when:
        one = createJobRequest()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createJobRequest()
        two = createJobRequest()

        then:
        one.hashCode() != two.hashCode()

        when:
        def jobMetadata = Mock(JobMetadata)
        def criteria = Mock(ExecutionResourceCriteria)
        def agentEnvironmentRequest = new AgentEnvironmentRequest.Builder().build()
        def agentConfigRequest = new AgentConfigRequest.Builder().build()
        def jobArchivalDataRequest = new JobArchivalDataRequest.Builder().build()
        one = new JobRequest(null, null, null, jobMetadata, criteria, agentEnvironmentRequest, agentConfigRequest, jobArchivalDataRequest)
        two = new JobRequest(null, null, null, jobMetadata, criteria, agentEnvironmentRequest, agentConfigRequest, jobArchivalDataRequest)

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        JobRequest one
        JobRequest two

        when:
        one = createJobRequest()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createJobRequest()
        two = createJobRequest()

        then:
        one.toString() != two.toString()

        when:
        def jobMetadata = Mock(JobMetadata)
        def criteria = Mock(ExecutionResourceCriteria)
        def agentEnvironmentRequest = new AgentEnvironmentRequest.Builder().build()
        def agentConfigRequest = new AgentConfigRequest.Builder().build()
        def jobArchivalDataRequest = new JobArchivalDataRequest.Builder().build()
        one = new JobRequest(null, null, null, jobMetadata, criteria, agentEnvironmentRequest, agentConfigRequest, jobArchivalDataRequest)
        two = new JobRequest(null, null, null, jobMetadata, criteria, agentEnvironmentRequest, agentConfigRequest, jobArchivalDataRequest)

        then:
        one.toString() == two.toString()
    }

    JobRequest createJobRequest() {
        def metadata = new JobMetadata.Builder(UUID.randomUUID().toString(), UUID.randomUUID().toString()).build()
        def criteria = new ExecutionResourceCriteria(
            Lists.newArrayList(new Criterion.Builder().withId(UUID.randomUUID().toString()).build()),
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(),
            null
        )
        def requestedId = UUID.randomUUID().toString()
        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def timeout = RandomSuppliers.INT.get()
        def interactive = true
        def archivingDisabled = true
        def requestedArchiveLocationPrefix = UUID.randomUUID().toString()
        def jobResources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def jobDirectoryLocation = "/tmp"
        def requestedEnvironmentVariables = ImmutableMap.of(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def requestedAgentEnvironment = new AgentEnvironmentRequest.Builder()
            .withRequestedEnvironmentVariables(requestedEnvironmentVariables)
            .withRequestedJobCpu(RandomSuppliers.INT.get())
            .withRequestedJobMemory(RandomSuppliers.INT.get())
            .withExt(GenieObjectMapper.getMapper().readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
            .build()
        def requestedAgentConfig = new AgentConfigRequest.Builder()
            .withArchivingDisabled(archivingDisabled)
            .withTimeoutRequested(timeout)
            .withInteractive(interactive)
            .withRequestedJobDirectoryLocation(jobDirectoryLocation)
            .withExt(GenieObjectMapper.getMapper().readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
            .build()

        def requestedJobArchivalData = new JobArchivalDataRequest.Builder()
            .withRequestedArchiveLocationPrefix(requestedArchiveLocationPrefix)
            .build()

        return new JobRequest(
            requestedId,
            jobResources,
            commandArgs,
            metadata,
            criteria,
            requestedAgentEnvironment,
            requestedAgentConfig,
            requestedJobArchivalData
        )
    }
}
