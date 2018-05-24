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
import com.netflix.genie.common.dto.ApplicationStatus
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
class ApplicationMetadataSpec extends Specification {

    def "Can build immutable ApplicationMetadata instance"() {
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def metadataJson = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def metadata = GenieObjectMapper.mapper.readTree(metadataJson)
        def status = ApplicationStatus.ACTIVE
        def type = UUID.randomUUID().toString()
        ApplicationMetadata applicationMetadata

        when:
        applicationMetadata = new ApplicationMetadata.Builder(name, user, version, status)
                .withDescription(description)
                .withTags(tags)
                .withMetadata(metadata)
                .withType(type)
                .build()

        then:
        applicationMetadata.getName() == name
        applicationMetadata.getUser() == user
        applicationMetadata.getStatus() == status
        applicationMetadata.getVersion() == version
        applicationMetadata.getDescription().orElse(UUID.randomUUID().toString()) == description
        applicationMetadata.getTags() == tags
        applicationMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
        applicationMetadata.getType().orElse(UUID.randomUUID().toString()) == type

        when:
        applicationMetadata = new ApplicationMetadata.Builder(name, user, version, status)
                .withMetadata(metadataJson)
                .build()

        then:
        applicationMetadata.getName() == name
        applicationMetadata.getUser() == user
        applicationMetadata.getStatus() == status
        applicationMetadata.getVersion() == version
        !applicationMetadata.getDescription().isPresent()
        applicationMetadata.getTags().isEmpty()
        applicationMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
        !applicationMetadata.getType().isPresent()

        when:
        applicationMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()

        then:
        applicationMetadata.getName() == name
        applicationMetadata.getUser() == user
        applicationMetadata.getStatus() == status
        applicationMetadata.getVersion() == version
        !applicationMetadata.getDescription().isPresent()
        applicationMetadata.getTags().isEmpty()
        !applicationMetadata.getMetadata().isPresent()
        !applicationMetadata.getType().isPresent()

        when: "Empty strings should result in not present"
        def newTags = Sets.newHashSet(tags)
        newTags.add("     ")
        applicationMetadata = new ApplicationMetadata.Builder(name, user, version, status)
                .withDescription("")
                .withTags(newTags)
                .withMetadata(metadata)
                .withType("\t")
                .build()

        then:
        applicationMetadata.getName() == name
        applicationMetadata.getUser() == user
        applicationMetadata.getStatus() == status
        applicationMetadata.getVersion() == version
        !applicationMetadata.getDescription().isPresent()
        applicationMetadata.getTags() == tags
        applicationMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
        !applicationMetadata.getType().isPresent()
    }

    def "Test equals"() {
        def base = createApplicationMetadata()
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
        comparable = new ApplicationMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ApplicationStatus.ACTIVE
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
        def status = ApplicationStatus.DEPRECATED
        base = new ApplicationMetadata.Builder(name, user, version, status).build()
        comparable = new ApplicationMetadata.Builder(name, user, version, status).build()

        then:
        base == comparable
    }

    def "Test hashCode"() {
        ApplicationMetadata one
        ApplicationMetadata two

        when:
        one = createApplicationMetadata()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createApplicationMetadata()
        two = createApplicationMetadata()

        then:
        one.hashCode() != two.hashCode()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ApplicationStatus.DEPRECATED
        one = new ApplicationMetadata.Builder(name, user, version, status).build()
        two = new ApplicationMetadata.Builder(name, user, version, status).build()

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        ApplicationMetadata one
        ApplicationMetadata two

        when:
        one = createApplicationMetadata()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createApplicationMetadata()
        two = createApplicationMetadata()

        then:
        one.toString() != two.toString()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ApplicationStatus.DEPRECATED
        one = new ApplicationMetadata.Builder(name, user, version, status).build()
        two = new ApplicationMetadata.Builder(name, user, version, status).build()

        then:
        one.toString() == two.toString()
    }

    ApplicationMetadata createApplicationMetadata() {
        return new ApplicationMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ApplicationStatus.ACTIVE
        )
                .withDescription(UUID.randomUUID().toString())
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withMetadata(GenieObjectMapper.mapper.readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
                .withType(UUID.randomUUID().toString())
                .build()
    }
}
