/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.util;

import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Tag;

import java.util.Set;

/**
 * Utility methods for metrics.
 *
 * @author mprimi
 * @since 3.1.0
 */
public final class MetricsUtils {

    private static final Tag SUCCESS_STATUS_TAG = Tag.of(
        MetricsConstants.TagKeys.STATUS,
        MetricsConstants.TagValues.SUCCESS
    );
    private static final Tag FAILURE_STATUS_TAG = Tag.of(
        MetricsConstants.TagKeys.STATUS,
        MetricsConstants.TagValues.FAILURE
    );

    /**
     * Utility class private constructor.
     */
    private MetricsUtils() {
    }

    /**
     * Convenience method that creates a tag set pre-populated with success status.
     *
     * @return a new set containing success tags
     */
    public static Set<Tag> newSuccessTagsSet() {
        final Set<Tag> tags = Sets.newHashSet();
        addSuccessTags(tags);
        return tags;
    }

    /**
     * Convenience method to add success tag to an existing set of tags.
     *
     * @param tags the set of tags to add success tags to
     */
    public static void addSuccessTags(final Set<Tag> tags) {
        tags.add(SUCCESS_STATUS_TAG);
    }

    /**
     * Convenience method that creates a tag set pre-populated with failure status and exception details.
     *
     * @param t the exception
     * @return a new set containing failure tags
     */
    public static Set<Tag> newFailureTagsSetForException(final Throwable t) {
        final Set<Tag> tags = Sets.newHashSet();
        addFailureTagsWithException(tags, t);
        return tags;
    }

    /**
     * Convenience method to add failure status and exception cause to an existing set of tags.
     *
     * @param tags      the set of existing tags
     * @param throwable the exception to be tagged
     */
    public static void addFailureTagsWithException(
        final Set<Tag> tags,
        final Throwable throwable
    ) {
        tags.add(FAILURE_STATUS_TAG);
        tags.add(Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, throwable.getClass().getCanonicalName()));
    }
}
