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

import com.google.common.collect.Lists
import com.netflix.genie.common.dto.CommandStatus
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.test.suppliers.RandomSuppliers
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link CommandRequest} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class CommandRequestSpec extends Specification {

    def "Can build immutable command request"() {
        def metadata = new CommandMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE
        ).build()
        def requestedId = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def executable = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def memory = 3_820
        def checkDelay = 380_324L
        CommandRequest request

        when:
        request = new CommandRequest.Builder(metadata, executable)
                .withRequestedId(requestedId)
                .withResources(resources)
                .withMemory(memory)
                .withCheckDelay(checkDelay)
                .build()

        then:
        request.getMetadata() == metadata
        request.getRequestedId().orElse(UUID.randomUUID().toString()) == requestedId
        request.getResources() == resources
        request.getExecutable() == executable
        request.getMemory().orElse(-1) == memory
        request.getCheckDelay().orElse(null) == checkDelay

        when:
        request = new CommandRequest.Builder(metadata, executable).build()

        then:
        request.getMetadata() == metadata
        !request.getRequestedId().isPresent()
        request.getResources() != null
        request.getExecutable() == executable
        !request.getMemory().isPresent()
        !request.getCheckDelay().isPresent()

        when: "Optional fields are blank they're ignored"
        def newExecutable = Lists.newArrayList(executable)
        newExecutable.add("\t")
        newExecutable.add(" ")
        newExecutable.add("")
        request = new CommandRequest.Builder(metadata, newExecutable)
                .withRequestedId(" ")
                .withResources(resources)
                .withMemory(memory)
                .withCheckDelay(checkDelay)
                .build()

        then:
        request.getMetadata() == metadata
        !request.getRequestedId().isPresent()
        request.getResources() == resources
        request.getExecutable() == executable
        request.getMemory().orElse(-1) == memory
        request.getCheckDelay().orElse(null) == checkDelay
    }

    def "Test equals"() {
        def base = createCommandRequest()
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
        comparable = new CommandRequest.Builder(Mock(CommandMetadata), Lists.newArrayList(UUID.randomUUID().toString()))
                .withRequestedId(UUID.randomUUID().toString())
                .toString()

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
        base = new CommandRequest.Builder(baseMetadata, Lists.newArrayList(binary)).build()
        comparable = new CommandRequest.Builder(comparableMetadata, Lists.newArrayList(binary)).build()

        then:
        base == comparable
    }

    def "Test hashCode"() {
        CommandRequest one
        CommandRequest two

        when:
        one = createCommandRequest()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createCommandRequest()
        two = createCommandRequest()

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
        one = new CommandRequest.Builder(baseMetadata, Lists.newArrayList(binary)).build()
        two = new CommandRequest.Builder(comparableMetadata, Lists.newArrayList(binary)).build()

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        CommandRequest one
        CommandRequest two

        when:
        one = createCommandRequest()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createCommandRequest()
        two = createCommandRequest()

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
        one = new CommandRequest.Builder(baseMetadata, Lists.newArrayList(binary)).build()
        two = new CommandRequest.Builder(comparableMetadata, Lists.newArrayList(binary)).build()

        then:
        one.toString() == two.toString()
    }

    CommandRequest createCommandRequest() {
        def metadata = new CommandMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE
        ).build()
        def requestedId = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        return new CommandRequest.Builder(metadata, Lists.newArrayList(UUID.randomUUID().toString()))
                .withRequestedId(requestedId)
                .withResources(resources)
                .withMemory(RandomSuppliers.INT.get())
                .withCheckDelay(RandomSuppliers.LONG.get())
                .build()
    }
}
