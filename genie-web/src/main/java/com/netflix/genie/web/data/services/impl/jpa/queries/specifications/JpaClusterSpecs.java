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
package com.netflix.genie.web.data.services.impl.jpa.queries.specifications;

import com.google.common.collect.Lists;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity_;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.jpa.domain.Specification;

import javax.annotation.Nullable;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Specifications for JPA queries.
 *
 * @author tgianos
 * @see <a href="http://tinyurl.com/n6nubvm">Docs</a>
 */
public final class JpaClusterSpecs {

    /**
     * Private constructor for utility class.
     */
    private JpaClusterSpecs() {
    }

    /**
     * Generate a specification given the parameters.
     *
     * @param name          The name of the cluster to find
     * @param statuses      The statuses of the clusters to find
     * @param tags          The tags of the clusters to find
     * @param minUpdateTime The minimum updated time of the clusters to find
     * @param maxUpdateTime The maximum updated time of the clusters to find
     * @return The specification
     */
    public static Specification<ClusterEntity> find(
        @Nullable final String name,
        @Nullable final Set<String> statuses,
        @Nullable final Set<TagEntity> tags,
        @Nullable final Instant minUpdateTime,
        @Nullable final Instant maxUpdateTime
    ) {
        return (final Root<ClusterEntity> root, final CriteriaQuery<?> cq, final CriteriaBuilder cb) -> {
            final List<Predicate> predicates = new ArrayList<>();
            if (StringUtils.isNotBlank(name)) {
                predicates.add(
                    JpaSpecificationUtils.getStringLikeOrEqualPredicate(cb, root.get(ClusterEntity_.name), name)
                );
            }
            if (minUpdateTime != null) {
                predicates.add(cb.greaterThanOrEqualTo(root.get(ClusterEntity_.updated), minUpdateTime));
            }
            if (maxUpdateTime != null) {
                predicates.add(cb.lessThan(root.get(ClusterEntity_.updated), maxUpdateTime));
            }
            if (tags != null && !tags.isEmpty()) {
                final Join<ClusterEntity, TagEntity> tagEntityJoin = root.join(ClusterEntity_.tags);
                predicates.add(tagEntityJoin.in(tags));
                cq.groupBy(root.get(ClusterEntity_.id));
                cq.having(cb.equal(cb.count(root.get(ClusterEntity_.id)), tags.size()));
            }
            if (statuses != null && !statuses.isEmpty()) {
                //Could optimize this as we know size could use native array
                predicates.add(
                    cb.or(
                        statuses
                            .stream()
                            .map(status -> cb.equal(root.get(ClusterEntity_.status), status))
                            .toArray(Predicate[]::new)
                    )
                );
            }

            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }

    /**
     * Get the specification for the query which will find the clusters which match the given criterion.
     *
     * @param criterion The {@link Criterion} to match clusters against
     * @return A {@link Specification} for this query
     */
    public static Specification<ClusterEntity> findClustersMatchingCriterion(final Criterion criterion) {
        return (final Root<ClusterEntity> root, final CriteriaQuery<?> cq, final CriteriaBuilder cb) ->
            JpaSpecificationUtils.createCriterionPredicate(
                root,
                cq,
                cb,
                ClusterEntity_.uniqueId,
                ClusterEntity_.name,
                ClusterEntity_.version,
                ClusterEntity_.status,
                () -> root.join(ClusterEntity_.tags, JoinType.INNER),
                ClusterEntity_.id,
                criterion
            );
    }

    /**
     * Get the specification for the query which will find the clusters which match any of the given criterion.
     *
     * @param criteria The set of {@link Criterion} to match clusters against
     * @return A {@link Specification} for this query
     */
    public static Specification<ClusterEntity> findClustersMatchingAnyCriterion(final Set<Criterion> criteria) {
        return (final Root<ClusterEntity> root, final CriteriaQuery<?> cq, final CriteriaBuilder cb) -> {
            final List<Predicate> predicates = Lists.newArrayList();

            for (final Criterion criterion : criteria) {
                final Subquery<Long> criterionSubquery = cq.subquery(Long.class);
                final Root<ClusterEntity> criterionSubqueryRoot = criterionSubquery.from(ClusterEntity.class);
                criterionSubquery.select(criterionSubqueryRoot.get(ClusterEntity_.id));
                criterionSubquery.where(
                    JpaSpecificationUtils.createCriterionPredicate(
                        criterionSubqueryRoot,
                        criterionSubquery,
                        cb,
                        ClusterEntity_.uniqueId,
                        ClusterEntity_.name,
                        ClusterEntity_.version,
                        ClusterEntity_.status,
                        () -> criterionSubqueryRoot.join(ClusterEntity_.tags, JoinType.INNER),
                        ClusterEntity_.id,
                        criterion
                    )
                );
                predicates.add(root.get(ClusterEntity_.id).in(criterionSubquery));
            }

            return cb.or(predicates.toArray(new Predicate[0]));
        };
    }
}
