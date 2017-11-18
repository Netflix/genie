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

import com.netflix.genie.core.jpa.entities.TagEntity;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Predicate;
import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * Utility methods for the specification classes.
 *
 * @author tgianos
 * @since 3.0.0
 */
public final class JpaSpecificationUtils {

    static final String PERCENT = "%";
    static final String TAG_DELIMITER = "|";

    private JpaSpecificationUtils() {
    }

    /**
     * Convert a set of TagEntities to the '|' delimited tag search string.
     *
     * @param tags The tags to convert
     * @return The tag search string in case insensitive order. e.g. |tag1||tag2||tag3|
     */
    public static String createTagSearchString(final Set<TagEntity> tags) {
        // Tag search string length max is currently 1024 which will be caught by hibernate validator if this
        // exceeds that length
        return TAG_DELIMITER
            + tags
            .stream()
            .map(TagEntity::getTag)
            .sorted(String.CASE_INSENSITIVE_ORDER)
            .reduce((one, two) -> one + TAG_DELIMITER + TAG_DELIMITER + two)
            .orElse("")
            + TAG_DELIMITER;
    }

    /**
     * Create either an equals or like predicate based on the presence of the '%' character in the search value.
     *
     * @param cb         The criteria builder to use for predicate creation
     * @param expression The expression of the field the predicate is acting on
     * @param value      The value to compare the field to
     * @return A LIKE predicate if the value contains a '%' otherwise an EQUAL predicate
     */
    static Predicate getStringLikeOrEqualPredicate(
        @NotNull final CriteriaBuilder cb,
        @NotNull final Expression<String> expression,
        @NotNull final String value
    ) {
        if (StringUtils.contains(value, PERCENT)) {
            return cb.like(expression, value);
        } else {
            return cb.equal(expression, value);
        }
    }

    /**
     * Get the sorted like statement for tags used in specification queries.
     *
     * @param tags The tags to use. Not null.
     * @return The tags sorted while ignoring case delimited with percent symbol.
     */
    static String getTagLikeString(@NotNull final Set<String> tags) {
        final StringBuilder builder = new StringBuilder();
        tags.stream()
            .filter(StringUtils::isNotBlank)
            .sorted(String.CASE_INSENSITIVE_ORDER)
            .forEach(
                tag -> builder
                    .append(PERCENT)
                    .append(TAG_DELIMITER)
                    .append(tag)
                    .append(TAG_DELIMITER)
            );
        return builder.append(PERCENT).toString();
    }
}
