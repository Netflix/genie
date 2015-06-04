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
package com.netflix.genie.server.repository.jpa;

import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.ClusterStatus;
import com.netflix.genie.common.model.Cluster_;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.common.model.Command_;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.jpa.domain.Specification;

/**
 * Specifications for JPA queries.
 *
 * @see <a href="http://tinyurl.com/n6nubvm">Docs</a>
 * @author tgianos
 */
public final class ClusterSpecs {

    /**
     * Private constructor for utility class.
     */
    protected ClusterSpecs() {
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
    public static Specification<Cluster> find(
            final String name,
            final Set<ClusterStatus> statuses,
            final Set<String> tags,
            final Long minUpdateTime,
            final Long maxUpdateTime) {
        return new Specification<Cluster>() {
            @Override
            public Predicate toPredicate(
                    final Root<Cluster> root,
                    final CriteriaQuery<?> cq,
                    final CriteriaBuilder cb) {
                final List<Predicate> predicates = new ArrayList<>();
                if (StringUtils.isNotBlank(name)) {
                    predicates.add(cb.like(root.get(Cluster_.name), name));
                }
                if (minUpdateTime != null) {
                    predicates.add(cb.greaterThanOrEqualTo(root.get(Cluster_.updated), new Date(minUpdateTime)));
                }
                if (maxUpdateTime != null) {
                    predicates.add(cb.lessThan(root.get(Cluster_.updated), new Date(maxUpdateTime)));
                }
                if (tags != null) {
                    for (final String tag : tags) {
                        if (StringUtils.isNotBlank(tag)) {
                            predicates.add(cb.isMember(tag, root.get(Cluster_.tags)));
                        }
                    }
                }
                if (statuses != null && !statuses.isEmpty()) {
                    //Could optimize this as we know size could use native array
                    final List<Predicate> orPredicates = new ArrayList<>();
                    for (final ClusterStatus status : statuses) {
                        orPredicates.add(cb.equal(root.get(Cluster_.status), status));
                    }
                    predicates.add(cb.or(orPredicates.toArray(new Predicate[orPredicates.size()])));
                }

                return cb.and(predicates.toArray(new Predicate[predicates.size()]));
            }
        };
    }

    /**
     * Get all the clusters given the specified parameters.
     *
     * @param clusterCriteria The cluster criteria
     * @param commandCriteria The command Criteria
     * @return The specification
     */
    public static Specification<Cluster> findByClusterAndCommandCriteria(
            final ClusterCriteria clusterCriteria,
            final Set<String> commandCriteria) {
        return new Specification<Cluster>() {
            @Override
            public Predicate toPredicate(
                    final Root<Cluster> root,
                    final CriteriaQuery<?> cq,
                    final CriteriaBuilder cb) {
                final List<Predicate> predicates = new ArrayList<>();
                final Join<Cluster, Command> commands = root.join(Cluster_.commands);

                cq.distinct(true);

                predicates.add(cb.equal(commands.get(Command_.status), CommandStatus.ACTIVE));
                predicates.add(cb.equal(root.get(Cluster_.status), ClusterStatus.UP));

                if (commandCriteria != null) {
                    for (final String tag : commandCriteria) {
                        predicates.add(cb.isMember(tag, commands.get(Command_.tags)));

                    }
                }

                if (clusterCriteria != null) {
                    for (final String tag : clusterCriteria.getTags()) {
                        predicates.add(cb.isMember(tag, root.get(Cluster_.tags)));
                    }
                }

                return cb.and(predicates.toArray(new Predicate[predicates.size()]));
            }
        };
    }

    /**
     * Get all the clusters given the specified parameters.
     *
     * @param commandId The id of the command that is registered with this cluster
     * @param statuses The status of the cluster
     * @return The specification
     */
    public static Specification<Cluster> findClustersForCommand(
            final String commandId,
            final Set<ClusterStatus> statuses) {
        return new Specification<Cluster>() {
            @Override
            public Predicate toPredicate(
                    final Root<Cluster> root,
                    final CriteriaQuery<?> cq,
                    final CriteriaBuilder cb) {
                final List<Predicate> predicates = new ArrayList<>();
                final Join<Cluster, Command> commands = root.join(Cluster_.commands);

                predicates.add(cb.equal(commands.get(Command_.id), commandId));

                if (statuses != null && !statuses.isEmpty()) {
                    //Could optimize this as we know size could use native array
                    final List<Predicate> orPredicates = new ArrayList<>();
                    for (final ClusterStatus status : statuses) {
                        orPredicates.add(cb.equal(root.get(Cluster_.status), status));
                    }
                    predicates.add(cb.or(orPredicates.toArray(new Predicate[orPredicates.size()])));
                }

                return cb.and(predicates.toArray(new Predicate[predicates.size()]));
            }
        };
    }
}
