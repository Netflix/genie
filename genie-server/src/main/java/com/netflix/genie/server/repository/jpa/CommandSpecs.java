/*
 * Copyright 2014 Netflix, Inc.
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

import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Command_;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.jpa.domain.Specification;

/**
 * Specifications for JPA queries.
 *
 * see http://tinyurl.com/n6nubvm
 * @author tgianos
 */
public final class CommandSpecs {

    /**
     * Private constructor for utility class.
     */
    private CommandSpecs() {
    }

    /**
     * Get a specification using the specified parameters.
     *
     * @param name The name of the command
     * @param userName The name of the user who created the command
     * @return A specification object used for querying
     */
    public static Specification<Command> findByNameAndUser(
            final String name, final String userName) {
        return new Specification<Command>() {
            @Override
            public Predicate toPredicate(
                    final Root<Command> root,
                    final CriteriaQuery<?> cq,
                    final CriteriaBuilder cb) {
                final List<Predicate> predicates = new ArrayList<Predicate>();
                if (StringUtils.isNotBlank(name)) {
                    predicates.add(cb.equal(root.get(Command_.name), name));
                }
                if (StringUtils.isNotBlank(userName)) {
                    predicates.add(cb.equal(root.get(Command_.user), userName));
                }
                return cb.and(predicates.toArray(new Predicate[0]));
            }
        };
    }
}
