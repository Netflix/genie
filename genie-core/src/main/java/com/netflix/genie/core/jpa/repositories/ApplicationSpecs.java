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
package com.netflix.genie.core.jpa.repositories;

import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.model.Application_;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
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
public final class ApplicationSpecs {

    /**
     * Private constructor for utility class.
     */
    protected ApplicationSpecs() {
    }

    /**
     * Get a specification using the specified parameters.
     *
     * @param name     The name of the application
     * @param userName The name of the user who created the application
     * @param statuses The status of the application
     * @param tags     The set of tags to search the command for
     * @return A specification object used for querying
     */
    public static Specification<Application> find(
            final String name, final String userName, final Set<ApplicationStatus> statuses, final Set<String> tags) {
        return (final Root<Application> root, final CriteriaQuery<?> cq, final CriteriaBuilder cb) -> {
            final List<Predicate> predicates = new ArrayList<>();
            if (StringUtils.isNotBlank(name)) {
                predicates.add(cb.equal(root.get(Application_.name), name));
            }
            if (StringUtils.isNotBlank(userName)) {
                predicates.add(cb.equal(root.get(Application_.user), userName));
            }
            if (statuses != null && !statuses.isEmpty()) {
                final List<Predicate> orPredicates =
                        statuses
                                .stream()
                                .map(status -> cb.equal(root.get(Application_.status), status))
                                .collect(Collectors.toList());
                predicates.add(cb.or(orPredicates.toArray(new Predicate[orPredicates.size()])));
            }
            if (tags != null) {
                tags.stream()
                        .filter(StringUtils::isNotBlank)
                        .forEach(tag -> predicates.add(cb.isMember(tag, root.get(Application_.tags))));
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
