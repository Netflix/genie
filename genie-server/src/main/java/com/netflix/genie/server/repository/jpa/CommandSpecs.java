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

import com.netflix.genie.common.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.persistence.criteria.*;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.jpa.domain.Specification;

/**
 * Specifications for JPA queries.
 *
 * @see <a href="http://tinyurl.com/n6nubvm">Docs</a>
 * @author tgianos
 */
public final class CommandSpecs {

    /**
     * Private constructor for utility class.
     */
    protected CommandSpecs() {
    }

    /**
     * Get a specification using the specified parameters.
     *
     * @param name     The name of the command
     * @param userName The name of the user who created the command
     * @param statuses The status of the command
     * @param tags     The set of tags to search the command for
     * @return A specification object used for querying
     */
    public static Specification<Command> find(
            final String name, final String userName, final Set<CommandStatus> statuses, final Set<String> tags) {
        return new Specification<Command>() {
            @Override
            public Predicate toPredicate(
                    final Root<Command> root,
                    final CriteriaQuery<?> cq,
                    final CriteriaBuilder cb) {
                final List<Predicate> predicates = new ArrayList<>();
                if (StringUtils.isNotBlank(name)) {
                    predicates.add(cb.equal(root.get(Command_.name), name));
                }
                if (StringUtils.isNotBlank(userName)) {
                    predicates.add(cb.equal(root.get(Command_.user), userName));
                }
                if (statuses != null && !statuses.isEmpty()) {
                    //Could optimize this as we know size could use native array
                    final List<Predicate> orPredicates = new ArrayList<>();
                    for (final CommandStatus status : statuses) {
                        orPredicates.add(cb.equal(root.get(Command_.status), status));
                    }
                    predicates.add(cb.or(orPredicates.toArray(new Predicate[orPredicates.size()])));
                }
                if (tags != null) {
                    for (final String tag : tags) {
                        if (StringUtils.isNotBlank(tag)) {
                            predicates.add(cb.isMember(tag, root.get(Command_.tags)));
                        }
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
    public static Specification<Cluster> findClusterForCommand(
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

                cq.distinct(true);

                if (commandId != null) {
                    predicates.add(cb.equal(commands.get(Command_.id), commandId));
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
}
