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
package com.netflix.genie.core.repositories.jpa;

import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Application_;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.common.model.Command_;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Specifications for JPA queries.
 *
 * @author tgianos
 * @see <a href="http://tinyurl.com/n6nubvm">Docs</a>
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
        return (final Root<Command> root, final CriteriaQuery<?> cq, final CriteriaBuilder cb) -> {
            final List<Predicate> predicates = new ArrayList<>();
            if (StringUtils.isNotBlank(name)) {
                predicates.add(cb.equal(root.get(Command_.name), name));
            }
            if (StringUtils.isNotBlank(userName)) {
                predicates.add(cb.equal(root.get(Command_.user), userName));
            }
            if (statuses != null && !statuses.isEmpty()) {
                final List<Predicate> orPredicates =
                        statuses
                                .stream()
                                .map(status -> cb.equal(root.get(Command_.status), status))
                                .collect(Collectors.toList());
                predicates.add(cb.or(orPredicates.toArray(new Predicate[orPredicates.size()])));
            }
            if (tags != null) {
                tags.stream()
                        .filter(StringUtils::isNotBlank)
                        .forEach(tag -> predicates.add(cb.isMember(tag, root.get(Command_.tags))));
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    /**
     * Get all the clusters given the specified parameters.
     *
     * @param applicationId The id of the application that is registered with these commands
     * @param statuses      The status of the commands
     * @return The specification
     */
    public static Specification<Command> findCommandsForApplication(
            final String applicationId,
            final Set<CommandStatus> statuses
    ) {
        return (final Root<Command> root, final CriteriaQuery<?> cq, final CriteriaBuilder cb) -> {
            final List<Predicate> predicates = new ArrayList<>();
            final Join<Command, Application> application = root.join(Command_.applications);

            predicates.add(cb.equal(application.get(Application_.id), applicationId));

            if (statuses != null && !statuses.isEmpty()) {
                final List<Predicate> orPredicates =
                        statuses
                                .stream()
                                .map(status -> cb.equal(root.get(Command_.status), status))
                                .collect(Collectors.toList());
                predicates.add(cb.or(orPredicates.toArray(new Predicate[orPredicates.size()])));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
