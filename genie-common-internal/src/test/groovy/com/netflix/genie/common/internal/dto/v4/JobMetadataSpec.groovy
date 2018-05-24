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
        jobUserMetadata.getVersion() == JobMetadata.DEFAULT_VERSION
        jobUserMetadata.getDescription().orElse(UUID.randomUUID().toString()) == description
        jobUserMetadata.getTags() == tags
        jobUserMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
        jobUserMetadata.getGroup().orElse(UUID.randomUUID().toString()) == group
        jobUserMetadata.getGrouping().orElse(UUID.randomUUID().toString()) == grouping
        jobUserMetadata.getGroupingInstance().orElse(UUID.randomUUID().toString()) == groupingInstance

        when:
        jobUserMetadata = new JobMetadata.Builder(name, user, version)
                .withMetadata(metadataJson)
                .build()

        then:
        jobUserMetadata.getName() == name
        jobUserMetadata.getUser() == user
        jobUserMetadata.getVersion() == version
        !jobUserMetadata.getDescription().isPresent()
        jobUserMetadata.getTags().isEmpty()
        jobUserMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
        !jobUserMetadata.getGroup().isPresent()
        !jobUserMetadata.getGrouping().isPresent()
        !jobUserMetadata.getGroupingInstance().isPresent()

        when:
        jobUserMetadata = new JobMetadata.Builder(name, user, version)
                .build()

        then:
        jobUserMetadata.getName() == name
        jobUserMetadata.getUser() == user
        jobUserMetadata.getVersion() == version
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
        jobUserMetadata.getVersion() == JobMetadata.DEFAULT_VERSION
        !jobUserMetadata.getDescription().isPresent()
        jobUserMetadata.getTags() == tags
        jobUserMetadata.getMetadata().orElse(Mock(JsonNode)) == metadata
        !jobUserMetadata.getGroup().isPresent()
        !jobUserMetadata.getGrouping().isPresent()
        !jobUserMetadata.getGroupingInstance().isPresent()
    }

    def "Test equals"() {
        def base = createJobMetadata()
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
        comparable = createJobMetadata()

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
        def description = UUID.randomUUID().toString()
        def tag = UUID.randomUUID().toString()
        def metadataJson = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def metadata = GenieObjectMapper.mapper.readTree(metadataJson)
        def group = UUID.randomUUID().toString()
        def email = UUID.randomUUID().toString() + "@" + UUID.randomUUID().toString() + ".com"
        def grouping = UUID.randomUUID().toString()
        def groupingInstance = UUID.randomUUID().toString()
        base = new JobMetadata.Builder(name, user, version)
                .withDescription(description)
                .withTags(Sets.newHashSet(tag))
                .withMetadata(metadata)
                .withGroup(group)
                .withEmail(email)
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .build()
        comparable = new JobMetadata.Builder(name, user, version)
                .withDescription(description)
                .withTags(Sets.newHashSet(tag))
                .withMetadata(metadata)
                .withGroup(group)
                .withEmail(email)
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .build()

        then:
        base == comparable
    }

    def "Test hashCode"() {
        JobMetadata one
        JobMetadata two

        when:
        one = createJobMetadata()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createJobMetadata()
        two = createJobMetadata()

        then:
        one.hashCode() != two.hashCode()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def tag = UUID.randomUUID().toString()
        def metadataJson = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def metadata = GenieObjectMapper.mapper.readTree(metadataJson)
        def group = UUID.randomUUID().toString()
        def email = UUID.randomUUID().toString() + "@" + UUID.randomUUID().toString() + ".com"
        def grouping = UUID.randomUUID().toString()
        def groupingInstance = UUID.randomUUID().toString()
        one = new JobMetadata.Builder(name, user, version)
                .withDescription(description)
                .withTags(Sets.newHashSet(tag))
                .withMetadata(metadata)
                .withGroup(group)
                .withEmail(email)
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .build()
        two = new JobMetadata.Builder(name, user, version)
                .withDescription(description)
                .withTags(Sets.newHashSet(tag))
                .withMetadata(metadata)
                .withGroup(group)
                .withEmail(email)
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .build()

        then:
        one.hashCode() == two.hashCode()
    }

    def "test toString"() {
        JobMetadata one
        JobMetadata two

        when:
        one = createJobMetadata()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createJobMetadata()
        two = createJobMetadata()

        then:
        one.toString() != two.toString()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def tag = UUID.randomUUID().toString()
        def metadataJson = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def metadata = GenieObjectMapper.mapper.readTree(metadataJson)
        def group = UUID.randomUUID().toString()
        def email = UUID.randomUUID().toString() + "@" + UUID.randomUUID().toString() + ".com"
        def grouping = UUID.randomUUID().toString()
        def groupingInstance = UUID.randomUUID().toString()
        one = new JobMetadata.Builder(name, user, version)
                .withDescription(description)
                .withTags(Sets.newHashSet(tag))
                .withMetadata(metadata)
                .withGroup(group)
                .withEmail(email)
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .build()
        two = new JobMetadata.Builder(name, user, version)
                .withDescription(description)
                .withTags(Sets.newHashSet(tag))
                .withMetadata(metadata)
                .withGroup(group)
                .withEmail(email)
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .build()

        then:
        one.toString() == two.toString()
    }

    JobMetadata createJobMetadata() {
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

        return new JobMetadata.Builder(name, user, version)
                .withDescription(description)
                .withTags(tags)
                .withMetadata(metadata)
                .withGroup(group)
                .withEmail(email)
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .build()
    }
}
