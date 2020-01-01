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
package com.netflix.genie.common.external.dtos.v4

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import spock.lang.Specification

/**
 * Specification tests for the {@link JobSpecification} class.
 *
 * @author tgianos
 */
class JobSpecificationSpec extends Specification {

    def "Can construct new job specification without optionals"() {
        def jobId = UUID.randomUUID().toString()
        def clusterId = UUID.randomUUID().toString()
        def commandId = UUID.randomUUID().toString()

        def job = new JobSpecification.ExecutionResource(jobId, new ExecutionEnvironment(null, null, null))
        def cluster = new JobSpecification.ExecutionResource(clusterId, new ExecutionEnvironment(null, null, null))
        def command = new JobSpecification.ExecutionResource(commandId, new ExecutionEnvironment(null, null, null))

        when:
        def jobSpecification = new JobSpecification(null, null, job, cluster, command, null, null, true, null, null, null)

        then:
        jobSpecification.getExecutableArgs().isEmpty()
        jobSpecification.getJobArgs().isEmpty()
        jobSpecification.getJob() == job
        jobSpecification.getCluster() == cluster
        jobSpecification.getCommand() == command
        jobSpecification.getApplications().isEmpty()
        jobSpecification.getEnvironmentVariables().isEmpty()
        jobSpecification.isInteractive()
        jobSpecification.getJobDirectoryLocation() == null
        !jobSpecification.getTimeout().isPresent()
    }

    def "Can construct new job specification with optionals"() {
        def jobId = UUID.randomUUID().toString()
        def clusterId = UUID.randomUUID().toString()
        def commandId = UUID.randomUUID().toString()
        def applicationId = UUID.randomUUID().toString()

        def commandArgs = Lists.newArrayList("one", "two", "three")
        def jobArgs = Lists.newArrayList("X", "Y", "Z")
        def job = new JobSpecification.ExecutionResource(jobId, new ExecutionEnvironment(null, null, null))
        def cluster = new JobSpecification.ExecutionResource(clusterId, new ExecutionEnvironment(null, null, null))
        def command = new JobSpecification.ExecutionResource(commandId, new ExecutionEnvironment(null, null, null))
        def applications = Lists.newArrayList(
            new JobSpecification.ExecutionResource(applicationId, new ExecutionEnvironment(null, null, null))
        )
        ImmutableMap<String, String> environmentVariables = ImmutableMap.of(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def jobDirectoryLocation = new File(".")
        def archiveLocation = UUID.randomUUID().toString()
        def timeout = 213_309

        when:
        def jobSpecification = new JobSpecification(
            commandArgs,
            jobArgs,
            job,
            cluster,
            command,
            applications,
            environmentVariables,
            false,
            jobDirectoryLocation,
            archiveLocation,
            timeout
        )

        then:
        jobSpecification.getExecutableArgs() == commandArgs
        jobSpecification.getJobArgs() == jobArgs
        jobSpecification.getJob() == job
        jobSpecification.getCluster() == cluster
        jobSpecification.getCommand() == command
        jobSpecification.getApplications() == applications
        jobSpecification.getEnvironmentVariables() == environmentVariables
        !jobSpecification.isInteractive()
        jobSpecification.getJobDirectoryLocation() == jobDirectoryLocation
        jobSpecification.getArchiveLocation() == Optional.of(archiveLocation)
        jobSpecification.getTimeout() == Optional.of(timeout)
    }

    def "Can construct new job specification with empty optionals"() {
        def jobId = UUID.randomUUID().toString()
        def clusterId = UUID.randomUUID().toString()
        def commandId = UUID.randomUUID().toString()
        def applicationId = UUID.randomUUID().toString()

        def commandArgs = Lists.newArrayList("one", "", "three")
        def jobArgs = Lists.newArrayList("X", "", "Z")
        def job = new JobSpecification.ExecutionResource(jobId, new ExecutionEnvironment(null, null, null))
        def cluster = new JobSpecification.ExecutionResource(clusterId, new ExecutionEnvironment(null, null, null))
        def command = new JobSpecification.ExecutionResource(commandId, new ExecutionEnvironment(null, null, null))
        def applications = Lists.newArrayList(
            new JobSpecification.ExecutionResource(applicationId, new ExecutionEnvironment(null, null, null))
        )
        ImmutableMap<String, String> environmentVariables = ImmutableMap.of(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def jobDirectoryLocation = new File(".")
        def archiveLocation = null
        def timeout = null

        when:
        def jobSpecification = new JobSpecification(
            commandArgs,
            jobArgs,
            job,
            cluster,
            command,
            applications,
            environmentVariables,
            false,
            jobDirectoryLocation,
            archiveLocation,
            timeout
        )

        then:
        jobSpecification.getExecutableArgs() == commandArgs
        jobSpecification.getJobArgs() == jobArgs
        jobSpecification.getJob() == job
        jobSpecification.getCluster() == cluster
        jobSpecification.getCommand() == command
        jobSpecification.getApplications() == applications
        jobSpecification.getEnvironmentVariables() == environmentVariables
        !jobSpecification.isInteractive()
        jobSpecification.getJobDirectoryLocation() == jobDirectoryLocation
        jobSpecification.getArchiveLocation() == Optional.empty()
        jobSpecification.getTimeout() == Optional.empty()
    }

    def "Can construct execution resource without optionals"() {
        def id = UUID.randomUUID().toString()

        when:
        def executionResource = new JobSpecification.ExecutionResource(id, new ExecutionEnvironment(null, null, null))

        then:
        executionResource.getId() == id
        !executionResource.getExecutionEnvironment().getSetupFile().isPresent()
        executionResource.getExecutionEnvironment().getConfigs().isEmpty()
        executionResource.getExecutionEnvironment().getDependencies().isEmpty()
    }

    def "Can construct execution resource with optionals"() {
        def id = UUID.randomUUID().toString()
        def setupFile = UUID.randomUUID().toString()
        def configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())

        when:
        def executionResource = new JobSpecification.ExecutionResource(id, new ExecutionEnvironment(configs, dependencies, setupFile))

        then:
        executionResource.getId() == id
        executionResource.getExecutionEnvironment().getSetupFile().orElse(UUID.randomUUID().toString()) == setupFile
        executionResource.getExecutionEnvironment().getConfigs() == configs
        executionResource.getExecutionEnvironment().getDependencies() == dependencies
    }

    def "Test equals"() {
        def base = createJobSpecification()
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
        comparable = createJobSpecification()

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString())
        def jobArgs = Lists.newArrayList()
        def job = Mock(JobSpecification.ExecutionResource)
        def cluster = Mock(JobSpecification.ExecutionResource)
        def command = Mock(JobSpecification.ExecutionResource)
        def application = Mock(JobSpecification.ExecutionResource)
        def environmentVariables = ImmutableMap.of(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def interactive = true
        def jobDirectoryLocation = new File(UUID.randomUUID().toString())
        def archiveLocation = UUID.randomUUID().toString()
        def timeout = 232_281

        base = new JobSpecification(
            commandArgs,
            jobArgs,
            job,
            cluster,
            command,
            Lists.newArrayList(application),
            environmentVariables,
            interactive,
            jobDirectoryLocation,
            archiveLocation,
            timeout
        )
        comparable = new JobSpecification(
            commandArgs,
            jobArgs,
            job,
            cluster,
            command,
            Lists.newArrayList(application),
            environmentVariables,
            interactive,
            jobDirectoryLocation,
            archiveLocation,
            timeout
        )

        then:
        base == comparable
    }

    def "Test hashCode"() {
        JobSpecification one
        JobSpecification two

        when:
        one = createJobSpecification()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createJobSpecification()
        two = createJobSpecification()

        then:
        one.hashCode() != two.hashCode()

        when:
        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString())
        def jobArgs = Lists.newArrayList()
        def job = Mock(JobSpecification.ExecutionResource)
        def cluster = Mock(JobSpecification.ExecutionResource)
        def command = Mock(JobSpecification.ExecutionResource)
        def application = Mock(JobSpecification.ExecutionResource)
        def environmentVariables = ImmutableMap.of(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def interactive = true
        def jobDirectoryLocation = new File(UUID.randomUUID().toString())
        def archiveLocation = UUID.randomUUID().toString()
        def timeout = 2_233

        one = new JobSpecification(
            commandArgs,
            jobArgs,
            job,
            cluster,
            command,
            Lists.newArrayList(application),
            environmentVariables,
            interactive,
            jobDirectoryLocation,
            archiveLocation,
            timeout
        )
        two = new JobSpecification(
            commandArgs,
            jobArgs,
            job,
            cluster,
            command,
            Lists.newArrayList(application),
            environmentVariables,
            interactive,
            jobDirectoryLocation,
            archiveLocation,
            timeout
        )

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        JobSpecification one
        JobSpecification two

        when:
        one = createJobSpecification()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createJobSpecification()
        two = createJobSpecification()

        then:
        one.toString() != two.toString()

        when:
        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString())
        def jobArgs = Lists.newArrayList()
        def job = Mock(JobSpecification.ExecutionResource)
        def cluster = Mock(JobSpecification.ExecutionResource)
        def command = Mock(JobSpecification.ExecutionResource)
        def application = Mock(JobSpecification.ExecutionResource)
        def environmentVariables = ImmutableMap.of(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def interactive = true
        def jobDirectoryLocation = new File(UUID.randomUUID().toString())
        def archiveLocation = UUID.randomUUID().toString()
        def timeout = 38_382

        one = new JobSpecification(
            commandArgs,
            jobArgs,
            job,
            cluster,
            command,
            Lists.newArrayList(application),
            environmentVariables,
            interactive,
            jobDirectoryLocation,
            archiveLocation,
            timeout
        )
        two = new JobSpecification(
            commandArgs,
            jobArgs,
            job,
            cluster,
            command,
            Lists.newArrayList(application),
            environmentVariables,
            interactive,
            jobDirectoryLocation,
            archiveLocation,
            timeout
        )

        then:
        one.toString() == two.toString()
    }

    JobSpecification createJobSpecification() {
        def jobId = UUID.randomUUID().toString()
        def clusterId = UUID.randomUUID().toString()
        def commandId = UUID.randomUUID().toString()
        def applicationId = UUID.randomUUID().toString()

        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def jobArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def job = new JobSpecification.ExecutionResource(jobId, new ExecutionEnvironment(null, null, null))
        def cluster = new JobSpecification.ExecutionResource(clusterId, new ExecutionEnvironment(null, null, null))
        def command = new JobSpecification.ExecutionResource(commandId, new ExecutionEnvironment(null, null, null))
        def applications = Lists.newArrayList(
            new JobSpecification.ExecutionResource(applicationId, new ExecutionEnvironment(null, null, null))
        )
        ImmutableMap<String, String> environmentVariables = ImmutableMap.of(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def jobDirectoryLocation = new File(".")
        def archiveLocation = UUID.randomUUID().toString()
        def timeout = 238_834_324

        return new JobSpecification(
            commandArgs,
            jobArgs,
            job,
            cluster,
            command,
            applications,
            environmentVariables,
            false,
            jobDirectoryLocation,
            archiveLocation,
            timeout
        )
    }
}
