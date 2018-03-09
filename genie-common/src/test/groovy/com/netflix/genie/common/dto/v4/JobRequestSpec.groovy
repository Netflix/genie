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
package com.netflix.genie.common.dto.v4

import com.google.common.collect.Lists
import spock.lang.Specification

/**
 * Specifications for the {@link JobRequest} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
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
        def disableArchival = true
        def jobResources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def jobDirectoryLocation = new File("/tmp")
        ApiJobRequest jobRequest

        when:
        jobRequest = new ApiJobRequest.Builder(metadata, criteria)
                .withRequestedId(requestedId)
                .withCommandArgs(commandArgs)
                .withDisableArchiving(disableArchival)
                .withTimeout(timeout)
                .withInteractive(interactive)
                .withResources(jobResources)
                .withJobDirectoryLocation(jobDirectoryLocation)
                .build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        jobRequest.getRequestedId().orElse(UUID.randomUUID().toString()) == requestedId
        jobRequest.getCommandArgs() == commandArgs
        jobRequest.isDisableArchiving()
        jobRequest.isInteractive()
        jobRequest.getTimeout().orElse(-1) == timeout
        jobRequest.getResources() == jobResources
        jobRequest.getJobDirectoryLocation().orElse(null) == jobDirectoryLocation

        when:
        jobRequest = new ApiJobRequest.Builder(metadata, criteria).build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        !jobRequest.isDisableArchiving()
        !jobRequest.isInteractive()
        !jobRequest.getTimeout().isPresent()
        jobRequest.getResources() != null
        !jobRequest.getJobDirectoryLocation().isPresent()

        when:
        jobRequest = new ApiJobRequest.Builder(metadata, criteria).withCommandArgs(null).build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        !jobRequest.isDisableArchiving()
        !jobRequest.isInteractive()
        !jobRequest.getTimeout().isPresent()
        jobRequest.getResources() != null
        !jobRequest.getJobDirectoryLocation().isPresent()
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
        def disableArchival = true
        def jobResources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def jobDirectoryLocation = new File("/tmp")
        AgentJobRequest jobRequest

        when:
        jobRequest = new AgentJobRequest.Builder(metadata, criteria, jobDirectoryLocation)
                .withRequestedId(requestedId)
                .withCommandArgs(commandArgs)
                .withDisableArchiving(disableArchival)
                .withTimeout(timeout)
                .withInteractive(interactive)
                .withResources(jobResources)
                .build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        jobRequest.getRequestedId().orElse(UUID.randomUUID().toString()) == requestedId
        jobRequest.getCommandArgs() == commandArgs
        jobRequest.isDisableArchiving()
        jobRequest.isInteractive()
        jobRequest.getTimeout().orElse(-1) == timeout
        jobRequest.getResources() == jobResources
        jobRequest.getJobDirectoryLocation().orElse(null) == jobDirectoryLocation

        when:
        jobRequest = new AgentJobRequest.Builder(metadata, criteria, jobDirectoryLocation).build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        !jobRequest.isDisableArchiving()
        !jobRequest.isInteractive()
        !jobRequest.getTimeout().isPresent()
        jobRequest.getResources() != null
        jobRequest.getJobDirectoryLocation().orElse(null) == jobDirectoryLocation

        when:
        jobRequest = new AgentJobRequest.Builder(metadata, criteria, jobDirectoryLocation)
                .withCommandArgs(null)
                .build()

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        !jobRequest.isDisableArchiving()
        !jobRequest.isInteractive()
        !jobRequest.getTimeout().isPresent()
        jobRequest.getResources() != null
        jobRequest.getJobDirectoryLocation().orElse(null) == jobDirectoryLocation
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
        def disableArchival = true
        def jobResources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def jobDirectoryLocation = new File("/tmp")
        JobRequest jobRequest

        when:
        jobRequest = new JobRequest(
                requestedId,
                jobResources,
                commandArgs,
                disableArchival,
                timeout,
                interactive,
                metadata,
                criteria,
                jobDirectoryLocation
        )

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        jobRequest.getRequestedId().orElse(UUID.randomUUID().toString()) == requestedId
        jobRequest.getCommandArgs() == commandArgs
        jobRequest.isDisableArchiving()
        jobRequest.isInteractive()
        jobRequest.getTimeout().orElse(-1) == timeout
        jobRequest.getResources() == jobResources
        jobRequest.getJobDirectoryLocation().orElse(null) == jobDirectoryLocation

        when:
        jobRequest = new JobRequest(
                null,
                null,
                null,
                false,
                null,
                false,
                metadata,
                criteria,
                null
        )

        then:
        jobRequest.getMetadata() == metadata
        jobRequest.getCriteria() == criteria
        !jobRequest.getRequestedId().isPresent()
        jobRequest.getCommandArgs().isEmpty()
        !jobRequest.isDisableArchiving()
        !jobRequest.isInteractive()
        !jobRequest.getTimeout().isPresent()
        jobRequest.getResources() != null
        !jobRequest.getJobDirectoryLocation().isPresent()
    }
}
