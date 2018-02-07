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
package com.netflix.genie.web.jpa.specifications;

import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.ApplicationEntity_;
import com.netflix.genie.web.jpa.entities.TagEntity;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.jpa.domain.Specification;

import javax.annotation.Nullable;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Specifications for JPA queries.
 *
 * @author tgianos
 * @see <a href="http://tinyurl.com/n6nubvm">Docs</a>
 */
public final class JpaApplicationSpecs {

    /**
     * Private constructor for utility class.
     */
    private JpaApplicationSpecs() {
    }

    /**
     * Get a specification using the specified parameters.
     *
     * @param name     The name of the application
     * @param user     The name of the user who created the application
     * @param statuses The status of the application
     * @param tags     The set of tags to search with
     * @param type     The type of applications to fine
     * @return A specification object used for querying
     */
    public static Specification<ApplicationEntity> find(
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<ApplicationStatus> statuses,
        @Nullable final Set<TagEntity> tags,
        @Nullable final String type
    ) {
        return (final Root<ApplicationEntity> root, final CriteriaQuery<?> cq, final CriteriaBuilder cb) -> {
            final List<Predicate> predicates = new ArrayList<>();
            if (StringUtils.isNotBlank(name)) {
                predicates.add(
                    JpaSpecificationUtils.getStringLikeOrEqualPredicate(cb, root.get(ApplicationEntity_.name), name)
                );
            }
            if (StringUtils.isNotBlank(user)) {
                predicates.add(
                    JpaSpecificationUtils.getStringLikeOrEqualPredicate(cb, root.get(ApplicationEntity_.user), user)
                );
            }
            if (statuses != null && !statuses.isEmpty()) {
                predicates.add(
                    cb.or(
                        statuses
                            .stream()
                            .map(status -> cb.equal(root.get(ApplicationEntity_.status), status))
                            .toArray(Predicate[]::new)
                    )
                );
            }
            if (tags != null && !tags.isEmpty()) {
                final Join<ApplicationEntity, TagEntity> tagEntityJoin = root.join(ApplicationEntity_.tags);
                predicates.add(tagEntityJoin.in(tags));
                cq.groupBy(root.get(ApplicationEntity_.id));
                cq.having(cb.equal(cb.count(root.get(ApplicationEntity_.id)), tags.size()));
            }
            if (StringUtils.isNotBlank(type)) {
                predicates.add(
                    JpaSpecificationUtils.getStringLikeOrEqualPredicate(cb, root.get(ApplicationEntity_.type), type)
                );
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
