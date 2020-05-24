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
package com.netflix.genie.web.data.services.impl.jpa.queries.predicates;

import com.google.common.collect.Lists;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.web.data.services.impl.jpa.entities.BaseEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.IdEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity_;
import com.netflix.genie.web.data.services.impl.jpa.entities.UniqueIdEntity;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.criteria.AbstractQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.SingularAttribute;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Utility methods for the JPA {@link Predicate} generation.
 *
 * @author tgianos
 * @since 3.0.0
 */
public final class PredicateUtils {

    static final String PERCENT = "%";
    static final String TAG_DELIMITER = "|";

    private PredicateUtils() {
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

    static <E extends BaseEntity> Predicate createCriterionPredicate(
        final Root<E> root,
        final AbstractQuery<?> cq,
        final CriteriaBuilder cb,
        final SingularAttribute<UniqueIdEntity, String> uniqueIdAttribute,
        final SingularAttribute<BaseEntity, String> nameAttribute,
        final SingularAttribute<BaseEntity, String> versionAttribute,
        final SingularAttribute<BaseEntity, String> statusAttribute,
        final Supplier<Join<E, TagEntity>> tagJoinSupplier,
        final SingularAttribute<IdEntity, Long> idAttribute,
        final Criterion criterion
    ) {
        final List<Predicate> predicates = Lists.newArrayList();

        criterion.getId().ifPresent(id -> predicates.add(cb.equal(root.get(uniqueIdAttribute), id)));
        criterion.getName().ifPresent(name -> predicates.add(cb.equal(root.get(nameAttribute), name)));
        criterion.getVersion().ifPresent(version -> predicates.add(cb.equal(root.get(versionAttribute), version)));
        criterion.getStatus().ifPresent(status -> predicates.add(cb.equal(root.get(statusAttribute), status)));

        final Set<String> tags = criterion.getTags();
        if (!tags.isEmpty()) {
            final Join<E, TagEntity> tagJoin = tagJoinSupplier.get();
            predicates.add(tagJoin.get(TagEntity_.tag).in(tags));

            cq.groupBy(root.get(idAttribute));
            cq.having(
                cb.equal(
                    cb.count(root.get(idAttribute)),
                    tags.size()
                )
            );
        }

        return cb.and(predicates.toArray(new Predicate[0]));
    }
}
