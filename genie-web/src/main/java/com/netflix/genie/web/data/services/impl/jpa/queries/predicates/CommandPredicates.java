/*
 * Copyright 2015 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */
package com.netflix.genie.web.data.services.impl.jpa.queries.predicates;

import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity_;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * {@link Predicate} generation helpers for querying {@link CommandEntity}.
 *
 * @author tgianos
 */
public final class CommandPredicates {

    /**
     * Private constructor for utility class.
     */
    private CommandPredicates() {
    }

    /**
     * Get a predicate using the specified parameters.
     *
     * @param root     The {@link Root} (from) for this query
     * @param cq       The {@link CriteriaQuery} instance this predicate is for
     * @param cb       The {@link CriteriaBuilder} for the query
     * @param name     The name of the command
     * @param user     The name of the user who created the command
     * @param statuses The status of the command
     * @param tags     The set of tags to search the command for
     * @return A {@link Predicate} object used for querying
     */
    public static Predicate find(
        final Root<CommandEntity> root,
        final CriteriaQuery<?> cq,
        final CriteriaBuilder cb,
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<String> statuses,
        @Nullable final Set<TagEntity> tags
    ) {
        final List<Predicate> predicates = new ArrayList<>();
        if (StringUtils.isNotBlank(name)) {
            predicates.add(
                PredicateUtils.getStringLikeOrEqualPredicate(cb, root.get(CommandEntity_.name), name)
            );
        }
        if (StringUtils.isNotBlank(user)) {
            predicates.add(
                PredicateUtils.getStringLikeOrEqualPredicate(cb, root.get(CommandEntity_.user), user)
            );
        }
        if (statuses != null && !statuses.isEmpty()) {
            predicates.add(
                cb.or(
                    statuses
                        .stream()
                        .map(status -> cb.equal(root.get(CommandEntity_.status), status))
                        .toArray(Predicate[]::new)
                )
            );
        }
        if (tags != null && !tags.isEmpty()) {
            final Join<CommandEntity, TagEntity> tagEntityJoin = root.join(CommandEntity_.tags);
            predicates.add(tagEntityJoin.in(tags));
            cq.groupBy(root.get(CommandEntity_.id));
            cq.having(cb.equal(cb.count(root.get(CommandEntity_.id)), tags.size()));
        }
        return cb.and(predicates.toArray(new Predicate[0]));
    }

    /**
     * Get the specification for the query which will find the commands which match the given criterion.
     *
     * @param root      The {@link Root} (from) for the query
     * @param cq        The {@link CriteriaQuery} instance
     * @param cb        The {@link CriteriaBuilder} instance
     * @param criterion The {@link Criterion} to match commands against
     * @return A {@link Predicate} for this query
     */
    public static Predicate findCommandsMatchingCriterion(
        final Root<CommandEntity> root,
        final CriteriaQuery<?> cq,
        final CriteriaBuilder cb,
        final Criterion criterion
    ) {
        final Subquery<Long> criterionSubquery = cq.subquery(Long.class);
        final Root<CommandEntity> criterionSubqueryRoot = criterionSubquery.from(CommandEntity.class);
        criterionSubquery.select(criterionSubqueryRoot.get(CommandEntity_.id));
        criterionSubquery.where(
            cb.and(
                PredicateUtils.createCriterionPredicate(
                    criterionSubqueryRoot,
                    criterionSubquery,
                    cb,
                    CommandEntity_.uniqueId,
                    CommandEntity_.name,
                    CommandEntity_.version,
                    CommandEntity_.status,
                    () -> criterionSubqueryRoot.join(CommandEntity_.tags, JoinType.INNER),
                    CommandEntity_.id,
                    criterion
                ),
                cb.isNotEmpty(criterionSubqueryRoot.get(CommandEntity_.clusterCriteria))
            )
        );

        return root.get(CommandEntity_.id).in(criterionSubquery);
    }
}
