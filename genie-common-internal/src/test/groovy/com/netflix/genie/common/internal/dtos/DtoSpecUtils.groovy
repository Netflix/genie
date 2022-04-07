/*
 *
 *  Copyright 2022 Netflix, Inc.
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
package com.netflix.genie.common.internal.dtos

import com.netflix.genie.common.external.util.GenieObjectMapper
import com.netflix.genie.test.suppliers.RandomSuppliers

import java.time.Instant

/**
 * Utility methods for DTO tests to remove repeated code.
 *
 * @author tgianos
 */
class DtoSpecUtils {

    static ComputeResources getRandomComputeResources() {
        return new ComputeResources.Builder()
            .withCpu(RandomSuppliers.INT.get())
            .withGpu(RandomSuppliers.INT.get())
            .withMemoryMb(RandomSuppliers.INT.get())
            .withDiskMb(RandomSuppliers.INT.get())
            .withNetworkMbps(RandomSuppliers.INT.get())
            .build()
    }

    static Image getRandomImage() {
        return new Image.Builder()
            .withName(UUID.randomUUID().toString())
            .withTag(UUID.randomUUID().toString())
            .build()
    }

    static Command getRandomCommand() {
        def metadata = new CommandMetadata.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            CommandStatus.ACTIVE
        ).build()
        def id = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def clusterCriteria = [new Criterion.Builder().withId(UUID.randomUUID().toString()).build()]
        return new Command(
            id,
            created,
            updated,
            resources,
            metadata,
            [UUID.randomUUID().toString()],
            clusterCriteria,
            getRandomComputeResources(),
            getRandomImage()
        )
    }

    static CommandRequest getRandomCommandRequest() {
        def metadata = new CommandMetadata.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            CommandStatus.ACTIVE
        ).build()
        def requestedId = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        return new CommandRequest.Builder(metadata, [UUID.randomUUID().toString()])
            .withRequestedId(requestedId)
            .withResources(resources)
            .withClusterCriteria([new Criterion.Builder().withId(UUID.randomUUID().toString()).build()])
            .withComputeResources(getRandomComputeResources())
            .withImage(getRandomImage())
            .build()
    }

    static JobRequest getRandomJobRequest() {
        def metadata = new JobMetadata.Builder(UUID.randomUUID().toString(), UUID.randomUUID().toString()).build()
        def criteria = new ExecutionResourceCriteria(
            [new Criterion.Builder().withId(UUID.randomUUID().toString()).build()],
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(),
            null
        )
        def requestedId = UUID.randomUUID().toString()
        def commandArgs = [UUID.randomUUID().toString(), UUID.randomUUID().toString()]
        def timeout = RandomSuppliers.INT.get()
        def interactive = true
        def archivingDisabled = true
        def jobResources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def jobDirectoryLocation = "/tmp"
        def requestedEnvironmentVariables = [
            hi: UUID.randomUUID().toString(),
            bye: UUID.randomUUID().toString()
        ]
        def requestedJobEnvironment = new JobEnvironmentRequest.Builder()
            .withRequestedEnvironmentVariables(requestedEnvironmentVariables)
            .withRequestedComputeResources(getRandomComputeResources())
            .withExt(GenieObjectMapper.getMapper().readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
            .build()
        def requestedAgentConfig = new AgentConfigRequest.Builder()
            .withArchivingDisabled(archivingDisabled)
            .withTimeoutRequested(timeout)
            .withInteractive(interactive)
            .withRequestedJobDirectoryLocation(jobDirectoryLocation)
            .withExt(GenieObjectMapper.getMapper().readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
            .build()

        return new JobRequest(
            requestedId,
            jobResources,
            commandArgs,
            metadata,
            criteria,
            requestedJobEnvironment,
            requestedAgentConfig
        )
    }
}
