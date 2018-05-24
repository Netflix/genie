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

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.CommandStatus
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link JobMetadata} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class CommandMetadataSpec extends Specification {

    def "Can build immutable ClusterMetadata instance"() {
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def metadataJson = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def metadata = GenieObjectMapper.mapper.readTree(metadataJson)
        def status = CommandStatus.DEPRECATED
        CommandMetadata commandMetadata

        when:
        commandMetadata = new CommandMetadata.Builder(name, user, version, status)
                .withDescription(description)
                .withTags(tags)
                .withMetadata(metadata)
                .build()

        then:
        commandMetadata.getName() == name
        commandMetadata.getUser() == user
        commandMetadata.getStatus() == status
        commandMetadata.getVersion() == version
        commandMetadata.getDescription().orElse(UUID.randomUUID().toString()) == description
        commandMetadata.getTags() == tags
        commandMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata

        when:
        commandMetadata = new CommandMetadata.Builder(name, user, version, status).withMetadata(metadataJson).build()

        then:
        commandMetadata.getName() == name
        commandMetadata.getUser() == user
        commandMetadata.getStatus() == status
        commandMetadata.getVersion() == version
        !commandMetadata.getDescription().isPresent()
        commandMetadata.getTags().isEmpty()
        commandMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata

        when:
        commandMetadata = new CommandMetadata.Builder(name, user, version, status).build()

        then:
        commandMetadata.getName() == name
        commandMetadata.getUser() == user
        commandMetadata.getStatus() == status
        commandMetadata.getVersion() == version
        !commandMetadata.getDescription().isPresent()
        commandMetadata.getTags().isEmpty()
        !commandMetadata.getMetadata().isPresent()

        when: "Empty strings should result in not present"
        def newTags = Sets.newHashSet(tags)
        newTags.add("     ")
        commandMetadata = new CommandMetadata.Builder(name, user, version, status)
                .withDescription("")
                .withTags(newTags)
                .withMetadata(metadata)
                .build()

        then:
        commandMetadata.getName() == name
        commandMetadata.getUser() == user
        commandMetadata.getStatus() == status
        commandMetadata.getVersion() == version
        !commandMetadata.getDescription().isPresent()
        commandMetadata.getTags() == tags
        commandMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
    }

    def "Test equals"() {
        def base = createCommandMetadata()
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
        comparable = new CommandMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.DEPRECATED
        ).build()

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
        base = new CommandMetadata.Builder(name, user, version, status).build()
        comparable = new CommandMetadata.Builder(name, user, version, status).build()

        then:
        base == comparable
    }

    def "Test hashCode"() {
        CommandMetadata one
        CommandMetadata two

        when:
        one = createCommandMetadata()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createCommandMetadata()
        two = createCommandMetadata()

        then:
        one.hashCode() != two.hashCode()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = CommandStatus.INACTIVE
        one = new CommandMetadata.Builder(name, user, version, status).build()
        two = new CommandMetadata.Builder(name, user, version, status).build()

        then:
        one.hashCode() == two.hashCode()
    }

    def "Test toString"() {
        CommandMetadata one
        CommandMetadata two

        when:
        one = createCommandMetadata()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createCommandMetadata()
        two = createCommandMetadata()

        then:
        one.toString() != two.toString()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = CommandStatus.DEPRECATED
        one = new CommandMetadata.Builder(name, user, version, status).build()
        two = new CommandMetadata.Builder(name, user, version, status).build()

        then:
        one.toString() == two.toString()
    }

    CommandMetadata createCommandMetadata() {
        return new CommandMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE
        )
                .withDescription(UUID.randomUUID().toString())
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withMetadata(GenieObjectMapper.mapper.readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
                .build()
    }
}
