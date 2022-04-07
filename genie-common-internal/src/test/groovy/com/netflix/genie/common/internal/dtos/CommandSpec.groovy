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
package com.netflix.genie.common.internal.dtos

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.test.suppliers.RandomSuppliers
import spock.lang.Specification

import java.time.Instant

/**
 * Specifications for the {@link Command} class.
 *
 * @author tgianos
 */
class CommandSpec extends Specification {

    def "Can build immutable command resource"() {
        def metadata = new CommandMetadata.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            CommandStatus.ACTIVE
        ).build()
        def id = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def created = Instant.now()
        def updated = Instant.now()
        def executable = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def clusterCriteria = Lists.newArrayList(
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withName(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withStatus(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withVersion(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build()
        )
        def computeResources = DtoSpecUtils.getRandomComputeResources()
        def image = DtoSpecUtils.getRandomImage()
        Command command

        when:
        command = new Command(
            id,
            created,
            updated,
            resources,
            metadata,
            executable,
            clusterCriteria,
            computeResources,
            image
        )

        then:
        command.getId() == id
        command.getCreated() == created
        command.getUpdated() == updated
        command.getResources() == resources
        command.getMetadata() == metadata
        command.getExecutable() == executable
        command.getClusterCriteria() == clusterCriteria
        command.getComputeResources() == computeResources
        command.getImage() == image

        when:
        command = new Command(
            id,
            created,
            updated,
            null,
            metadata,
            executable,
            null,
            null,
            null
        )

        then:
        command.getId() == id
        command.getCreated() == created
        command.getUpdated() == updated
        command.getResources() == new ExecutionEnvironment(null, null, null)
        command.getMetadata() == metadata
        command.getExecutable() == executable
        command.getClusterCriteria().isEmpty()
        command.getComputeResources() == new ComputeResources.Builder().build()
        command.getImage() == new Image.Builder().build()

        when: "Executables contain blank strings they are removed"
        def newExecutable = Lists.newArrayList(executable)
        newExecutable.add("\t")
        newExecutable.add("   ")
        command = new Command(
            id,
            created,
            updated,
            null,
            metadata,
            newExecutable,
            null,
            null,
            null
        )

        then:
        command.getId() == id
        command.getCreated() == created
        command.getUpdated() == updated
        command.getResources() == new ExecutionEnvironment(null, null, null)
        command.getMetadata() == metadata
        command.getExecutable() == executable
        command.getClusterCriteria().isEmpty()
        command.getComputeResources() == new ComputeResources.Builder().build()
        command.getImage() == new Image.Builder().build()
    }

    def "Test equals"() {
        def base = DtoSpecUtils.getRandomCommand()
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
        comparable = new Command(
            UUID.randomUUID().toString(),
            Instant.now(),
            Instant.now(),
            null,
            Mock(CommandMetadata),
            Lists.newArrayList(UUID.randomUUID().toString()),
            null,
            null,
            null
        )

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = CommandStatus.INACTIVE
        def binary = UUID.randomUUID().toString()
        def baseMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        def id = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        def memory = RandomSuppliers.INT.get()
        base = new Command(
            id,
            created,
            updated,
            null,
            baseMetadata,
            Lists.newArrayList(binary),
            null,
            new ComputeResources.Builder().withMemoryMb(memory).build(),
            null
        )
        comparable = new Command(
            id,
            created,
            updated,
            null,
            comparableMetadata,
            Lists.newArrayList(binary),
            null,
            new ComputeResources.Builder().withMemoryMb(memory).build(),
            null
        )

        then:
        base == comparable
    }

    def "Test hashCode"() {
        Command one
        Command two

        when:
        one = DtoSpecUtils.getRandomCommand()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = DtoSpecUtils.getRandomCommand()
        two = DtoSpecUtils.getRandomCommand()

        then:
        one.hashCode() != two.hashCode()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = CommandStatus.INACTIVE
        def binary = UUID.randomUUID().toString()
        def baseMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        def id = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        def memory = RandomSuppliers.INT.get()
        one = new Command(
            id,
            created,
            updated,
            null,
            baseMetadata,
            Lists.newArrayList(binary),
            null,
            new ComputeResources.Builder().withMemoryMb(memory).build(),
            null
        )
        two = new Command(
            id,
            created,
            updated,
            null,
            comparableMetadata,
            Lists.newArrayList(binary),
            null,
            new ComputeResources.Builder().withMemoryMb(memory).build(),
            null
        )

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        Command one
        Command two

        when:
        one = DtoSpecUtils.getRandomCommand()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = DtoSpecUtils.getRandomCommand()
        two = DtoSpecUtils.getRandomCommand()

        then:
        one.toString() != two.toString()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = CommandStatus.INACTIVE
        def binary = UUID.randomUUID().toString()
        def baseMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        def id = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        def memory = RandomSuppliers.INT.get()
        one = new Command(
            id,
            created,
            updated,
            null,
            baseMetadata,
            Lists.newArrayList(binary),
            null,
            new ComputeResources.Builder().withMemoryMb(memory).build(),
            null
        )
        two = new Command(
            id,
            created,
            updated,
            null,
            comparableMetadata,
            Lists.newArrayList(binary),
            null,
            new ComputeResources.Builder().withMemoryMb(memory).build(),
            null
        )

        then:
        one.toString() == two.toString()
    }
}
