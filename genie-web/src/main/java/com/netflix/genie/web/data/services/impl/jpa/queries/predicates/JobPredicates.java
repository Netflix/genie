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

import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.JobEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.JobEntity_;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * {@link Predicate} helpers for querying {@link JobEntity}.
 *
 * @author tgianos
 */
public final class JobPredicates {

    /**
     * Protected constructor for utility class.
     */
    private JobPredicates() {
    }

    /**
     * Generate a criteria query predicate for a where clause based on the given parameters.
     *
     * @param root             The root to use
     * @param cb               The criteria builder to use
     * @param id               The job id
     * @param name             The job name
     * @param user             The user who created the job
     * @param statuses         The job statuses
     * @param tags             The tags for the jobs to find
     * @param clusterName      The cluster name
     * @param cluster          The cluster the job should have been run on
     * @param commandName      The command name
     * @param command          The command the job should have been run with
     * @param minStarted       The time which the job had to start after in order to be return (inclusive)
     * @param maxStarted       The time which the job had to start before in order to be returned (exclusive)
     * @param minFinished      The time which the job had to finish after in order to be return (inclusive)
     * @param maxFinished      The time which the job had to finish before in order to be returned (exclusive)
     * @param grouping         The job grouping to search for
     * @param groupingInstance The job grouping instance to search for
     * @return The specification
     */
    @SuppressWarnings("checkstyle:parameternumber")
    public static Predicate getFindPredicate(
        final Root<JobEntity> root,
        final CriteriaBuilder cb,
        @Nullable final String id,
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<String> statuses,
        @Nullable final Set<String> tags,
        @Nullable final String clusterName,
        @Nullable final ClusterEntity cluster,
        @Nullable final String commandName,
        @Nullable final CommandEntity command,
        @Nullable final Instant minStarted,
        @Nullable final Instant maxStarted,
        @Nullable final Instant minFinished,
        @Nullable final Instant maxFinished,
        @Nullable final String grouping,
        @Nullable final String groupingInstance
    ) {
        final List<Predicate> predicates = new ArrayList<>();
        if (StringUtils.isNotBlank(id)) {
            predicates.add(PredicateUtils.getStringLikeOrEqualPredicate(cb, root.get(JobEntity_.uniqueId), id));
        }
        if (StringUtils.isNotBlank(name)) {
            predicates.add(PredicateUtils.getStringLikeOrEqualPredicate(cb, root.get(JobEntity_.name), name));
        }
        if (StringUtils.isNotBlank(user)) {
            predicates.add(PredicateUtils.getStringLikeOrEqualPredicate(cb, root.get(JobEntity_.user), user));
        }
        if (statuses != null && !statuses.isEmpty()) {
            predicates.add(
                cb.or(
                    statuses
                        .stream()
                        .map(status -> cb.equal(root.get(JobEntity_.status), status)).toArray(Predicate[]::new)
                )
            );
        }
        if (tags != null && !tags.isEmpty()) {
            predicates.add(cb.like(root.get(JobEntity_.tagSearchString), PredicateUtils.getTagLikeString(tags)));
        }
        if (cluster != null) {
            predicates.add(cb.equal(root.get(JobEntity_.cluster), cluster));
        }
        if (StringUtils.isNotBlank(clusterName)) {
            predicates.add(
                PredicateUtils.getStringLikeOrEqualPredicate(cb, root.get(JobEntity_.clusterName), clusterName)
            );
        }
        if (command != null) {
            predicates.add(cb.equal(root.get(JobEntity_.command), command));
        }
        if (StringUtils.isNotBlank(commandName)) {
            predicates.add(
                PredicateUtils.getStringLikeOrEqualPredicate(cb, root.get(JobEntity_.commandName), commandName)
            );
        }
        if (minStarted != null) {
            predicates.add(cb.greaterThanOrEqualTo(root.get(JobEntity_.started), minStarted));
        }
        if (maxStarted != null) {
            predicates.add(cb.lessThan(root.get(JobEntity_.started), maxStarted));
        }
        if (minFinished != null) {
            predicates.add(cb.greaterThanOrEqualTo(root.get(JobEntity_.finished), minFinished));
        }
        if (maxFinished != null) {
            predicates.add(cb.lessThan(root.get(JobEntity_.finished), maxFinished));
        }
        if (grouping != null) {
            predicates.add(
                PredicateUtils.getStringLikeOrEqualPredicate(cb, root.get(JobEntity_.grouping), grouping)
            );
        }
        if (groupingInstance != null) {
            predicates.add(
                PredicateUtils.getStringLikeOrEqualPredicate(
                    cb,
                    root.get(JobEntity_.groupingInstance),
                    groupingInstance
                )
            );
        }
        return cb.and(predicates.toArray(new Predicate[0]));
    }
}
