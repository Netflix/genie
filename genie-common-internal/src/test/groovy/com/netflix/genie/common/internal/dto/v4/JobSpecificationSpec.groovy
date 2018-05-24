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
import com.google.common.collect.Sets
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specification tests for the {@link JobSpecification} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class JobSpecificationSpec extends Specification {

    def "Can construct new job specification without optionals"() {
        def jobId = UUID.randomUUID().toString()
        def clusterId = UUID.randomUUID().toString()
        def commandId = UUID.randomUUID().toString()

        def job = new JobSpecification.ExecutionResource(jobId, new ExecutionEnvironment(null, null, null))
        def cluster = new JobSpecification.ExecutionResource(clusterId, new ExecutionEnvironment(null, null, null))
        def command = new JobSpecification.ExecutionResource(commandId, new ExecutionEnvironment(null, null, null))

        when:
        def jobSpecification = new JobSpecification(null, job, cluster, command, null, null, true, null)

        then:
        jobSpecification.getCommandArgs().isEmpty()
        jobSpecification.getJob() == job
        jobSpecification.getCluster() == cluster
        jobSpecification.getCommand() == command
        jobSpecification.getApplications().isEmpty()
        jobSpecification.getEnvironmentVariables().isEmpty()
        jobSpecification.isInteractive()
        jobSpecification.getJobDirectoryLocation() == null
    }

    def "Can construct new job specification with optionals"() {
        def jobId = UUID.randomUUID().toString()
        def clusterId = UUID.randomUUID().toString()
        def commandId = UUID.randomUUID().toString()
        def applicationId = UUID.randomUUID().toString()

        def commandArgs = Lists.newArrayList("one", "two", "three")
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

        when:
        def jobSpecification = new JobSpecification(
                commandArgs,
                job,
                cluster,
                command,
                applications,
                environmentVariables,
                false,
                jobDirectoryLocation
        )

        then:
        jobSpecification.getCommandArgs() == commandArgs
        jobSpecification.getJob() == job
        jobSpecification.getCluster() == cluster
        jobSpecification.getCommand() == command
        jobSpecification.getApplications() == applications
        jobSpecification.getEnvironmentVariables() == environmentVariables
        !jobSpecification.isInteractive()
        jobSpecification.getJobDirectoryLocation() == jobDirectoryLocation
    }

    def "Can construct new job specification with empty optionals"() {
        def jobId = UUID.randomUUID().toString()
        def clusterId = UUID.randomUUID().toString()
        def commandId = UUID.randomUUID().toString()
        def applicationId = UUID.randomUUID().toString()

        def commandArgs = Lists.newArrayList("one", "two", "three")
        def emptyCommandArgs = Lists.newArrayList(commandArgs)
        emptyCommandArgs.add("\n\n")
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

        when:
        def jobSpecification = new JobSpecification(
                emptyCommandArgs,
                job,
                cluster,
                command,
                applications,
                environmentVariables,
                false,
                jobDirectoryLocation
        )

        then:
        jobSpecification.getCommandArgs() == commandArgs
        jobSpecification.getJob() == job
        jobSpecification.getCluster() == cluster
        jobSpecification.getCommand() == command
        jobSpecification.getApplications() == applications
        jobSpecification.getEnvironmentVariables() == environmentVariables
        !jobSpecification.isInteractive()
        jobSpecification.getJobDirectoryLocation() == jobDirectoryLocation
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
        def commandArg = UUID.randomUUID().toString()
        def job = Mock(JobSpecification.ExecutionResource)
        def cluster = Mock(JobSpecification.ExecutionResource)
        def command = Mock(JobSpecification.ExecutionResource)
        def application = Mock(JobSpecification.ExecutionResource)
        def environmentVariables = ImmutableMap.of(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def interactive = true
        def jobDirectoryLocation = new File(UUID.randomUUID().toString())
        base = new JobSpecification(
                Lists.newArrayList(commandArg),
                job,
                cluster,
                command,
                Lists.newArrayList(application),
                environmentVariables,
                interactive,
                jobDirectoryLocation
        )
        comparable = new JobSpecification(
                Lists.newArrayList(commandArg),
                job,
                cluster,
                command,
                Lists.newArrayList(application),
                environmentVariables,
                interactive,
                jobDirectoryLocation
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
        def commandArg = UUID.randomUUID().toString()
        def job = Mock(JobSpecification.ExecutionResource)
        def cluster = Mock(JobSpecification.ExecutionResource)
        def command = Mock(JobSpecification.ExecutionResource)
        def application = Mock(JobSpecification.ExecutionResource)
        def environmentVariables = ImmutableMap.of(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def interactive = true
        def jobDirectoryLocation = new File(UUID.randomUUID().toString())
        one = new JobSpecification(
                Lists.newArrayList(commandArg),
                job,
                cluster,
                command,
                Lists.newArrayList(application),
                environmentVariables,
                interactive,
                jobDirectoryLocation
        )
        two = new JobSpecification(
                Lists.newArrayList(commandArg),
                job,
                cluster,
                command,
                Lists.newArrayList(application),
                environmentVariables,
                interactive,
                jobDirectoryLocation
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
        def commandArg = UUID.randomUUID().toString()
        def job = Mock(JobSpecification.ExecutionResource)
        def cluster = Mock(JobSpecification.ExecutionResource)
        def command = Mock(JobSpecification.ExecutionResource)
        def application = Mock(JobSpecification.ExecutionResource)
        def environmentVariables = ImmutableMap.of(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def interactive = true
        def jobDirectoryLocation = new File(UUID.randomUUID().toString())
        one = new JobSpecification(
                Lists.newArrayList(commandArg),
                job,
                cluster,
                command,
                Lists.newArrayList(application),
                environmentVariables,
                interactive,
                jobDirectoryLocation
        )
        two = new JobSpecification(
                Lists.newArrayList(commandArg),
                job,
                cluster,
                command,
                Lists.newArrayList(application),
                environmentVariables,
                interactive,
                jobDirectoryLocation
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

        return new JobSpecification(
                commandArgs,
                job,
                cluster,
                command,
                applications,
                environmentVariables,
                false,
                jobDirectoryLocation
        )
    }
}
