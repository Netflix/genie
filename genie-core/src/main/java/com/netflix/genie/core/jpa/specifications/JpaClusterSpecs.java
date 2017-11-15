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
package com.netflix.genie.core.jpa.specifications;

import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.ClusterEntity_;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity_;
import com.netflix.genie.core.jpa.entities.TagEntity;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.jpa.domain.Specification;

import javax.annotation.Nullable;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    JpaClusterSpecs() {
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
        @Nullable final Set<ClusterStatus> statuses,
        @Nullable final Set<TagEntity> tags,
        @Nullable final Date minUpdateTime,
        @Nullable final Date maxUpdateTime
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
                final List<Predicate> orPredicates =
                    statuses
                        .stream()
                        .map(status -> cb.equal(root.get(ClusterEntity_.status), status))
                        .collect(Collectors.toList());
                predicates.add(cb.or(orPredicates.toArray(new Predicate[orPredicates.size()])));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    /**
     * Get all the clusters given the specified parameters.
     *
     * @param clusterCriteria The cluster criteria
     * @param commandCriteria The command Criteria
     * @return The specification
     */
    //TODO
    public static Specification<ClusterEntity> findByClusterAndCommandCriteria(
        final ClusterCriteria clusterCriteria,
        final Set<String> commandCriteria
    ) {
        return (final Root<ClusterEntity> root, final CriteriaQuery<?> cq, final CriteriaBuilder cb) -> {
            final List<Predicate> predicates = new ArrayList<>();
            final Join<ClusterEntity, CommandEntity> commands = root.join(ClusterEntity_.commands);

            cq.distinct(true);

            predicates.add(cb.equal(root.get(ClusterEntity_.status), ClusterStatus.UP));

//            if (clusterCriteria.getTags() != null && !clusterCriteria.getTags().isEmpty()) {
//                predicates.add(
//                    cb.like(
//                        root.get(ClusterEntity_.tags),
//                        JpaSpecificationUtils.getTagLikeString(clusterCriteria.getTags())
//                    )
//                );
//            }
//
//            predicates.add(cb.equal(commands.get(CommandEntity_.status), CommandStatus.ACTIVE));
//
//            if (!commandCriteria.isEmpty()) {
//                predicates.add(
//                    cb.like(
//                        commands.get(CommandEntity_.tags),
//                        JpaSpecificationUtils.getTagLikeString(commandCriteria)
//                    )
//                );
//            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    /**
     * Get all the clusters given the specified parameters.
     *
     * @param commandId The id of the command that is registered with this cluster
     * @param statuses  The status of the cluster
     * @return The specification
     */
    public static Specification<ClusterEntity> findClustersForCommand(
        final String commandId,
        @Nullable final Set<ClusterStatus> statuses
    ) {
        return (final Root<ClusterEntity> root, final CriteriaQuery<?> cq, final CriteriaBuilder cb) -> {
            final List<Predicate> predicates = new ArrayList<>();
            final Join<ClusterEntity, CommandEntity> commands = root.join(ClusterEntity_.commands);

            predicates.add(cb.equal(commands.get(CommandEntity_.uniqueId), commandId));

            if (statuses != null && !statuses.isEmpty()) {
                //Could optimize this as we know size could use native array
                final List<Predicate> orPredicates =
                    statuses.stream()
                        .map(status -> cb.equal(root.get(ClusterEntity_.status), status))
                        .collect(Collectors.toList());
                predicates.add(cb.or(orPredicates.toArray(new Predicate[orPredicates.size()])));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
