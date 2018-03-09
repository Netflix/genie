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
        clusterMetadata = new ClusterMetadata.Builder(name, user, status)
                .withVersion(version)
                .withDescription(description)
                .withTags(tags)
                .withMetadata(metadata)
                .build()

        then:
        clusterMetadata.getName() == name
        clusterMetadata.getUser() == user
        clusterMetadata.getStatus() == status
        clusterMetadata.getVersion().orElse(UUID.randomUUID().toString()) == version
        clusterMetadata.getDescription().orElse(UUID.randomUUID().toString()) == description
        clusterMetadata.getTags() == tags
        clusterMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata

        when:
        clusterMetadata = new ClusterMetadata.Builder(name, user, status).withMetadata(metadataJson).build()

        then:
        clusterMetadata.getName() == name
        clusterMetadata.getUser() == user
        clusterMetadata.getStatus() == status
        !clusterMetadata.getVersion().isPresent()
        !clusterMetadata.getDescription().isPresent()
        clusterMetadata.getTags().isEmpty()
        clusterMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata

        when:
        clusterMetadata = new ClusterMetadata.Builder(name, user, status).build()

        then:
        clusterMetadata.getName() == name
        clusterMetadata.getUser() == user
        clusterMetadata.getStatus() == status
        !clusterMetadata.getVersion().isPresent()
        !clusterMetadata.getDescription().isPresent()
        clusterMetadata.getTags().isEmpty()
        !clusterMetadata.getMetadata().isPresent()
    }
}
