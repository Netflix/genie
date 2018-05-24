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

import com.google.common.collect.Sets
import com.netflix.genie.common.exceptions.GeniePreconditionException
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link Criterion} DTO.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class CriterionSpec extends Specification {

    def "Exception thrown when invalid criterion creation is attempted"() {
        when:
        new Criterion.Builder().withId(null).withName(null).withVersion(null).withStatus(null).withTags(null).build()

        then:
        thrown(GeniePreconditionException)

        when:
        new Criterion.Builder().withId("").withName("").withVersion("").withStatus("").withTags(Sets.newHashSet()).build()

        then:
        thrown(GeniePreconditionException)

        when:
        new Criterion.Builder().build()

        then:
        thrown(GeniePreconditionException)
    }

    def "Can create valid criterion"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = UUID.randomUUID().toString()
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Criterion criterion

        when:
        criterion = new Criterion.Builder()
                .withId(id)
                .withName(name)
                .withVersion(version)
                .withStatus(status)
                .withTags(tags)
                .build()

        then:
        criterion.getId().orElse(UUID.randomUUID().toString()) == id
        criterion.getName().orElse(UUID.randomUUID().toString()) == name
        criterion.getVersion().orElse(UUID.randomUUID().toString()) == version
        criterion.getStatus().orElse(UUID.randomUUID().toString()) == status
        criterion.getTags() == tags

        when:
        criterion = new Criterion.Builder()
                .withName(name)
                .withVersion(version)
                .withStatus(status)
                .withTags(tags)
                .build()

        then:
        !criterion.getId().isPresent()
        criterion.getName().orElse(UUID.randomUUID().toString()) == name
        criterion.getVersion().orElse(UUID.randomUUID().toString()) == version
        criterion.getStatus().orElse(UUID.randomUUID().toString()) == status
        criterion.getTags() == tags

        when:
        criterion = new Criterion.Builder()
                .withId(id)
                .withVersion(version)
                .withStatus(status)
                .withTags(tags)
                .build()

        then:
        criterion.getId().orElse(UUID.randomUUID().toString()) == id
        !criterion.getName().isPresent()
        criterion.getVersion().orElse(UUID.randomUUID().toString()) == version
        criterion.getStatus().orElse(UUID.randomUUID().toString()) == status
        criterion.getTags() == tags

        when:
        criterion = new Criterion.Builder()
                .withId(id)
                .withName(name)
                .withStatus(status)
                .withTags(tags)
                .build()

        then:
        criterion.getId().orElse(UUID.randomUUID().toString()) == id
        criterion.getName().orElse(UUID.randomUUID().toString()) == name
        !criterion.getVersion().isPresent()
        criterion.getStatus().orElse(UUID.randomUUID().toString()) == status
        criterion.getTags() == tags

        when:
        criterion = new Criterion.Builder()
                .withId(id)
                .withName(name)
                .withVersion(version)
                .withTags(tags)
                .build()

        then:
        criterion.getId().orElse(UUID.randomUUID().toString()) == id
        criterion.getName().orElse(UUID.randomUUID().toString()) == name
        criterion.getVersion().orElse(UUID.randomUUID().toString()) == version
        !criterion.getStatus().isPresent()
        criterion.getTags() == tags

        when:
        criterion = new Criterion.Builder()
                .withId(id)
                .withName(name)
                .withVersion(version)
                .withStatus(status)
                .build()

        then:
        criterion.getId().orElse(UUID.randomUUID().toString()) == id
        criterion.getName().orElse(UUID.randomUUID().toString()) == name
        criterion.getVersion().orElse(UUID.randomUUID().toString()) == version
        criterion.getStatus().orElse(UUID.randomUUID().toString()) == status
        criterion.getTags().isEmpty()

        when:
        criterion = new Criterion.Builder()
                .withId(id)
                .withName(name)
                .withVersion(version)
                .withStatus(status)
                .withTags(tags)
                .build()
        tags.add(UUID.randomUUID().toString())

        then:
        criterion.getId().orElse(UUID.randomUUID().toString()) == id
        criterion.getName().orElse(UUID.randomUUID().toString()) == name
        criterion.getVersion().orElse(UUID.randomUUID().toString()) == version
        criterion.getStatus().orElse(UUID.randomUUID().toString()) == status
        criterion.getTags() != tags
    }

    def "Empty strings and blank tags are treated as not present"() {
        when:
        new Criterion.Builder()
                .withId("\t")
                .withName(" ")
                .withVersion("\n\t")
                .withStatus("\n")
                .withTags(Sets.newHashSet(""))
                .build()

        then:
        thrown(GeniePreconditionException)

        when:
        def criterion = new Criterion.Builder()
                .withId("\t")
                .withName(" ")
                .withVersion("\n\t")
                .withStatus("\n")
                .withTags(Sets.newHashSet("valid tag", " "))
                .build()

        then:
        !criterion.getId().isPresent()
        !criterion.getName().isPresent()
        !criterion.getVersion().isPresent()
        !criterion.getStatus().isPresent()
        criterion.getTags() == Sets.newHashSet("valid tag")
    }

    def "Test equals"() {
        def base = createCriterion()
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
        comparable = new Criterion.Builder()
                .withName(UUID.randomUUID().toString())
                .withStatus(UUID.randomUUID().toString())
                .withVersion(UUID.randomUUID().toString())
                .withId(UUID.randomUUID().toString())
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build()

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def name = UUID.randomUUID().toString()
        def status = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def id = UUID.randomUUID().toString()
        def tag = UUID.randomUUID().toString()
        base = new Criterion.Builder()
                .withName(name)
                .withStatus(status)
                .withVersion(version)
                .withId(id)
                .withTags(Sets.newHashSet(tag))
                .build()
        comparable = new Criterion.Builder()
                .withName(name)
                .withStatus(status)
                .withVersion(version)
                .withId(id)
                .withTags(Sets.newHashSet(tag))
                .build()

        then:
        base == comparable
    }

    def "Test hashCode"() {
        Criterion one
        Criterion two

        when:
        one = createCriterion()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createCriterion()
        two = createCriterion()

        then:
        one.hashCode() != two.hashCode()

        when:
        def name = UUID.randomUUID().toString()
        def status = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def id = UUID.randomUUID().toString()
        def tag = UUID.randomUUID().toString()
        one = new Criterion.Builder()
                .withName(name)
                .withStatus(status)
                .withVersion(version)
                .withId(id)
                .withTags(Sets.newHashSet(tag))
                .build()
        two = new Criterion.Builder()
                .withName(name)
                .withStatus(status)
                .withVersion(version)
                .withId(id)
                .withTags(Sets.newHashSet(tag))
                .build()

        then:
        one.hashCode() == two.hashCode()
    }

    def "Test toString"() {
        Criterion one
        Criterion two

        when:
        one = createCriterion()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createCriterion()
        two = createCriterion()

        then:
        one.toString() != two.toString()

        when:
        def name = UUID.randomUUID().toString()
        def status = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def id = UUID.randomUUID().toString()
        def tag = UUID.randomUUID().toString()
        one = new Criterion.Builder()
                .withName(name)
                .withStatus(status)
                .withVersion(version)
                .withId(id)
                .withTags(Sets.newHashSet(tag))
                .build()
        two = new Criterion.Builder()
                .withName(name)
                .withStatus(status)
                .withVersion(version)
                .withId(id)
                .withTags(Sets.newHashSet(tag))
                .build()

        then:
        one.toString() == two.toString()
    }

    Criterion createCriterion() {
        return new Criterion.Builder()
                .withName(UUID.randomUUID().toString())
                .withStatus(UUID.randomUUID().toString())
                .withVersion(UUID.randomUUID().toString())
                .withId(UUID.randomUUID().toString())
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build()
    }
}
