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

import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.common.model.Job_;
import java.util.ArrayList;
import java.util.Date;
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
public final class JobSpecs {

    /**
     * Private constructor for utility class.
     */
    private JobSpecs() {
    }

    /**
     * Find jobs based on the parameters.
     *
     * @param id The job id
     * @param jobName The job name
     * @param userName The user who created the job
     * @param status The job status
     * @param clusterName The cluster name
     * @param clusterId The cluster id
     * @return The specification
     */
    public static Specification<Job> find(
            final String id,
            final String jobName,
            final String userName,
            final JobStatus status,
            final String clusterName,
            final String clusterId) {
        return new Specification<Job>() {
            @Override
            public Predicate toPredicate(
                    final Root<Job> root,
                    final CriteriaQuery<?> cq,
                    final CriteriaBuilder cb) {
                final List<Predicate> predicates = new ArrayList<Predicate>();
                if (StringUtils.isNotBlank(id)) {
                    predicates.add(cb.like(root.get(Job_.id), id));
                }
                if (StringUtils.isNotBlank(jobName)) {
                    predicates.add(cb.like(root.get(Job_.name), jobName));
                }
                if (StringUtils.isNotBlank(userName)) {
                    predicates.add(cb.equal(root.get(Job_.user), userName));
                }
                if (status != null) {
                    predicates.add(cb.equal(root.get(Job_.status), status));
                }
                if (StringUtils.isNotBlank(clusterName)) {
                    predicates.add(cb.equal(root.get(Job_.executionClusterName), clusterName));
                }
                if (StringUtils.isNotBlank(clusterId)) {
                    predicates.add(cb.equal(root.get(Job_.executionClusterId), clusterId));
                }
                return cb.and(predicates.toArray(new Predicate[0]));
            }
        };
    }

    /**
     * Find jobs that are zombies.
     *
     * @param currentTime The current time
     * @param zombieTime The time that zombies should be marked by
     * @return The specification for this query
     */
    public static Specification<Job> findZombies(
            final long currentTime,
            final long zombieTime) {
        return new Specification<Job>() {
            @Override
            public Predicate toPredicate(
                    final Root<Job> root,
                    final CriteriaQuery<?> cq,
                    final CriteriaBuilder cb) {
                final List<Predicate> predicates = new ArrayList<Predicate>();
                predicates.add(cb.lessThan(root.get(Job_.updated), new Date(currentTime - zombieTime)));
                final Predicate orPredicate1 = cb.equal(root.get(Job_.status), JobStatus.RUNNING);
                final Predicate orPredicate2 = cb.equal(root.get(Job_.status), JobStatus.INIT);
                predicates.add(cb.or(orPredicate1, orPredicate2));
                return cb.and(predicates.toArray(new Predicate[0]));
            }
        };
    }
}
