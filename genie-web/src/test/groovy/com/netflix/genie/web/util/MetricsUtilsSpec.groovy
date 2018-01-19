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

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Maps
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class MetricsUtilsSpec extends Specification {
    Map<String, String> initialMap
    final static String FOO = "foo"
    final static String BAR = "bar"

    void setup() {
        initialMap = Maps.newHashMap()
        initialMap.put(FOO, BAR)
    }

    void cleanup() {
    }

    def "AddFailureTagsWithException"() {
        setup:
        def exception = new RuntimeException("test")
        when:
        def taggedMap = MetricsUtils.addFailureTagsWithException(initialMap, exception)
        then:
        taggedMap == initialMap
        taggedMap == ImmutableMap.of(
                FOO, BAR,
                MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.FAILURE,
                MetricsConstants.TagKeys.EXCEPTION_CLASS, exception.getClass().getCanonicalName()
        )
    }

    def "AddFailureTagsToNullMap"() {
        setup:
        def exception = new RuntimeException("test")
        when:
        MetricsUtils.addFailureTagsWithException(null, exception)
        then:
        thrown(NullPointerException)
    }

    def "AddFailureTagsToNullException"() {
        setup:
        def exception = new RuntimeException("test")
        when:
        MetricsUtils.addFailureTagsWithException(initialMap, null)
        then:
        thrown(NullPointerException)
    }

    def "AddSuccessTags"() {
        setup:
        def exception = new RuntimeException("test")
        when:
        def taggedMap = MetricsUtils.addSuccessTags(initialMap)
        then:
        taggedMap == initialMap
        taggedMap == ImmutableMap.of(
                FOO, BAR,
                MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS,
        )
    }

    def "AddSuccessTagsToNullMap"() {
        setup:
        def exception = new RuntimeException("test")
        when:
        MetricsUtils.addSuccessTags(null)
        then:
        thrown(NullPointerException)
    }


    def "NewSuccessTagsMap"() {
        when:
        def taggedMap = MetricsUtils.newSuccessTagsMap()
        then:
        taggedMap == ImmutableMap.of(
                MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS,
        )
    }

    def "NewFailureTagsMapForException"() {
        setup:
        def exception = new RuntimeException("test")
        when:
        def taggedMap = MetricsUtils.newFailureTagsMapForException(exception)
        then:
        taggedMap == ImmutableMap.of(
                MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.FAILURE,
                MetricsConstants.TagKeys.EXCEPTION_CLASS, exception.getClass().getCanonicalName()
        )
    }
}
