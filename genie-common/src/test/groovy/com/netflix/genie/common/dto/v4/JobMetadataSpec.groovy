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
class JobMetadataSpec extends Specification {

    def "Can build immutable JobUserMetadata"() {
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def metadataJson = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def metadata = GenieObjectMapper.mapper.readTree(metadataJson)
        def group = UUID.randomUUID().toString()
        def email = UUID.randomUUID().toString() + "@" + UUID.randomUUID().toString() + ".com"
        def grouping = UUID.randomUUID().toString()
        def groupingInstance = UUID.randomUUID().toString()
        JobMetadata jobUserMetadata

        when:
        jobUserMetadata = new JobMetadata.Builder(name, user)
                .withVersion(version)
                .withDescription(description)
                .withTags(tags)
                .withMetadata(metadata)
                .withGroup(group)
                .withEmail(email)
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .build()

        then:
        jobUserMetadata.getName() == name
        jobUserMetadata.getUser() == user
        jobUserMetadata.getVersion().orElse(UUID.randomUUID().toString()) == version
        jobUserMetadata.getDescription().orElse(UUID.randomUUID().toString()) == description
        jobUserMetadata.getTags() == tags
        jobUserMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
        jobUserMetadata.getGroup().orElse(UUID.randomUUID().toString()) == group
        jobUserMetadata.getGrouping().orElse(UUID.randomUUID().toString()) == grouping
        jobUserMetadata.getGroupingInstance().orElse(UUID.randomUUID().toString()) == groupingInstance

        when:
        jobUserMetadata = new JobMetadata.Builder(name, user)
                .withMetadata(metadataJson)
                .build()

        then:
        jobUserMetadata.getName() == name
        jobUserMetadata.getUser() == user
        !jobUserMetadata.getVersion().isPresent()
        !jobUserMetadata.getDescription().isPresent()
        jobUserMetadata.getTags().isEmpty()
        jobUserMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
        !jobUserMetadata.getGroup().isPresent()
        !jobUserMetadata.getGrouping().isPresent()
        !jobUserMetadata.getGroupingInstance().isPresent()

        when:
        jobUserMetadata = new JobMetadata.Builder(name, user)
                .build()

        then:
        jobUserMetadata.getName() == name
        jobUserMetadata.getUser() == user
        !jobUserMetadata.getVersion().isPresent()
        !jobUserMetadata.getDescription().isPresent()
        jobUserMetadata.getTags().isEmpty()
        !jobUserMetadata.getMetadata().isPresent()
        !jobUserMetadata.getGroup().isPresent()
        !jobUserMetadata.getGrouping().isPresent()
        !jobUserMetadata.getGroupingInstance().isPresent()

        when: "Empty optional fields are supplied they're ignored"
        def newTags = Sets.newHashSet(tags)
        newTags.add(" \t")
        jobUserMetadata = new JobMetadata.Builder(name, user)
                .withVersion("")
                .withDescription(" ")
                .withTags(newTags)
                .withMetadata(metadata)
                .withGroup("\n")
                .withEmail("")
                .withGrouping("\t\t")
                .withGroupingInstance("\n\n")
                .build()

        then:
        jobUserMetadata.getName() == name
        jobUserMetadata.getUser() == user
        !jobUserMetadata.getVersion().isPresent()
        !jobUserMetadata.getDescription().isPresent()
        jobUserMetadata.getTags() == tags
        jobUserMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
        !jobUserMetadata.getGroup().isPresent()
        !jobUserMetadata.getGrouping().isPresent()
        !jobUserMetadata.getGroupingInstance().isPresent()
    }
}
