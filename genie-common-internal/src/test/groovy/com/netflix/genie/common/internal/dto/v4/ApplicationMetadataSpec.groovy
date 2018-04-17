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
}
