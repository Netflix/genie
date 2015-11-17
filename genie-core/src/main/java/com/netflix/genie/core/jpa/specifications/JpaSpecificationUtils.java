/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.jpa.specifications;

import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * Utility methods for the specification classes.
 *
 * @author tgianos
 * @since 3.0.0
 */
public final class JpaSpecificationUtils {
    private JpaSpecificationUtils() {
    }

    /**
     * Get the sorted like statement for tags used in specification queries.
     *
     * @param tags The tags to use. Not null.
     * @return The tags sorted while ignoring case delimited with percent symbol.
     */
    public static String getTagLikeString(@NotNull final Set<String> tags) {
        final StringBuilder builder = new StringBuilder();
        builder.append("%");
        tags.stream()
                .filter(StringUtils::isNotBlank)
                .sorted(String.CASE_INSENSITIVE_ORDER)
                .forEach(tag -> builder.append(tag).append("%"));
        return builder.toString();
    }
}
