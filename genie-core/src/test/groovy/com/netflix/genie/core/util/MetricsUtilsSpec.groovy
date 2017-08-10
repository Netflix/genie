package com.netflix.genie.core.util

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Maps
import spock.lang.Specification

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
