/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.util

import com.google.common.collect.ImmutableSet
import com.google.common.collect.Sets
import io.micrometer.core.instrument.Tag
import spock.lang.Specification

/**
 * Specifications for the Metrics Utility class.
 *
 * @author mprimi
 */
class MetricsUtilsSpec extends Specification {
    Tag fooBarTag = Tag.of("foo", "bar")
    Set<Tag> initialTags = Sets.newHashSet(fooBarTag)

    def "Can add failure tags for exception"() {
        setup:
        def exception = new RuntimeException("test")
        when:
        MetricsUtils.addFailureTagsWithException(initialTags, exception)
        then:
        initialTags == ImmutableSet.of(
            fooBarTag,
            Tag.of(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.FAILURE),
            Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, exception.getClass().getCanonicalName())
        )
    }

    def "Can add success tags to an empty set"() {
        setup:
        Set<Tag> emptyTags = Sets.newHashSet()
        when:
        MetricsUtils.addSuccessTags(emptyTags)
        then:
        emptyTags == Sets.newHashSet(
            Tag.of(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS)
        )
    }


    def "Can get a new success tags set"() {
        when:
        def finalTags = MetricsUtils.newSuccessTagsSet()
        then:
        finalTags == ImmutableSet.of(
            Tag.of(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS)
        )
    }

    def "Can get a new tag set for a given exception"() {
        setup:
        def exception = new RuntimeException("test")
        when:
        def finalTags = MetricsUtils.newFailureTagsSetForException(exception)
        then:
        finalTags == ImmutableSet.of(
            Tag.of(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.FAILURE),
            Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, exception.getClass().getCanonicalName())
        )
    }
}
