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
import com.netflix.genie.common.dto.ClusterStatus
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
class ClusterMetadataSpec extends Specification {

    def "Can build immutable ClusterMetadata instance"() {
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def metadataJson = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def metadata = GenieObjectMapper.mapper.readTree(metadataJson)
        def status = ClusterStatus.UP
        ClusterMetadata clusterMetadata

        when:
        clusterMetadata = new ClusterMetadata.Builder(name, user, version, status)
                .withDescription(description)
                .withTags(tags)
                .withMetadata(metadata)
                .build()

        then:
        clusterMetadata.getName() == name
        clusterMetadata.getUser() == user
        clusterMetadata.getStatus() == status
        clusterMetadata.getVersion() == version
        clusterMetadata.getDescription().orElse(UUID.randomUUID().toString()) == description
        clusterMetadata.getTags() == tags
        clusterMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata

        when:
        clusterMetadata = new ClusterMetadata.Builder(name, user, version, status).withMetadata(metadataJson).build()

        then:
        clusterMetadata.getName() == name
        clusterMetadata.getUser() == user
        clusterMetadata.getStatus() == status
        clusterMetadata.getVersion() == version
        !clusterMetadata.getDescription().isPresent()
        clusterMetadata.getTags().isEmpty()
        clusterMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata

        when:
        clusterMetadata = new ClusterMetadata.Builder(name, user, version, status).build()

        then:
        clusterMetadata.getName() == name
        clusterMetadata.getUser() == user
        clusterMetadata.getStatus() == status
        clusterMetadata.getVersion() == version
        !clusterMetadata.getDescription().isPresent()
        clusterMetadata.getTags().isEmpty()
        !clusterMetadata.getMetadata().isPresent()

        when: "Empty strings should result in not present"
        def newTags = Sets.newHashSet(tags)
        newTags.add("     ")
        clusterMetadata = new ClusterMetadata.Builder(name, user, version, status)
                .withDescription("")
                .withTags(newTags)
                .withMetadata(metadata)
                .build()

        then:
        clusterMetadata.getName() == name
        clusterMetadata.getUser() == user
        clusterMetadata.getStatus() == status
        clusterMetadata.getVersion() == version
        !clusterMetadata.getDescription().isPresent()
        clusterMetadata.getTags() == tags
        clusterMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
    }

    def "Test equals"() {
        def base = createClusterMetadata()
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
        comparable = new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.UP
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
        def status = ClusterStatus.UP
        base = new ClusterMetadata.Builder(name, user, version, status).build()
        comparable = new ClusterMetadata.Builder(name, user, version, status).build()

        then:
        base == comparable
    }

    def "Test hashCode"() {
        ClusterMetadata one
        ClusterMetadata two

        when:
        one = createClusterMetadata()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createClusterMetadata()
        two = createClusterMetadata()

        then:
        one.hashCode() != two.hashCode()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ClusterStatus.OUT_OF_SERVICE
        one = new ClusterMetadata.Builder(name, user, version, status).build()
        two = new ClusterMetadata.Builder(name, user, version, status).build()

        then:
        one.hashCode() == two.hashCode()
    }

    def "Test toString"() {
        ClusterMetadata one
        ClusterMetadata two

        when:
        one = createClusterMetadata()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createClusterMetadata()
        two = createClusterMetadata()

        then:
        one.toString() != two.toString()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ClusterStatus.TERMINATED
        one = new ClusterMetadata.Builder(name, user, version, status).build()
        two = new ClusterMetadata.Builder(name, user, version, status).build()

        then:
        one.toString() == two.toString()
    }

    ClusterMetadata createClusterMetadata() {
        return new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.UP
        )
                .withDescription(UUID.randomUUID().toString())
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withMetadata(GenieObjectMapper.mapper.readTree("{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"))
                .build()
    }
}
