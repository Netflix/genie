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

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.entities.projections.IdProjection;
import com.netflix.genie.core.jpa.entities.projections.JobHostNameProjection;
import com.netflix.genie.core.jpa.entities.projections.JobProjection;
import com.netflix.genie.core.jpa.entities.projections.UserJobAmountAggregateProjection;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Job repository.
 *
 * @author tgianos
 */
public interface JpaJobRepository extends JpaBaseRepository<JobEntity> {

    // TODO: Make interfaces generic but be aware of https://jira.spring.io/browse/DATAJPA-1185

    /**
     * Find jobs by host name and status.
     *
     * @param hostName The host name to search for
     * @param statuses The job statuses to filter by
     * @return The jobs
     */
    Set<JobProjection> findByHostNameAndStatusIn(final String hostName, final Set<JobStatus> statuses);

    /**
     * Find the jobs with one of the statuses entered.
     *
     * @param statuses The statuses to search
     * @return The job information requested
     */
    Set<JobHostNameProjection> findByStatusIn(final Set<JobStatus> statuses);

    /**
     * Find the distinct host names which currently have jobs in the given statuses.
     *
     * @param statuses The statuses to search
     * @return The set of hostnames
     */
    @Query("SELECT DISTINCT(j.hostName) FROM JobEntity j WHERE j.status IN (:statuses)")
    Set<String> findDistinctHostsWithJobsInStatuses(@Param("statuses") @NotEmpty final Set<JobStatus> statuses);

    /**
     * Deletes all jobs for the given ids.
     *
     * @param ids list of ids for which the jobs should be deleted
     * @return no. of jobs deleted
     */
    @Modifying
    Long deleteByIdIn(@NotNull final List<Long> ids);

    /**
     * Count all jobs that belong to a given user and are in any of the given states.
     *
     * @param user     the user name
     * @param statuses the set of statuses
     * @return the count of jobs matching the search criteria
     */
    Long countJobsByUserAndStatusIn(@NotBlank final String user, @NotEmpty final Set<JobStatus> statuses);

    /**
     * Returns the slice of ids for job requests created before the given date.
     *
     * @param date     The date before which the job requests were created
     * @param pageable The page of data to get
     * @return List of job request ids
     */
    // TODO: Explore deleteFirst{N}ByCreatedBefore
    Slice<IdProjection> findByCreatedBefore(@NotNull final Date date, @NotNull Pageable pageable);

    /**
     * Produce a count of jobs in the given states for each user.
     *
     * @param statuses the job statuses to filter by
     * @return a set of {@link UserJobAmountAggregateProjection}
     */
    @Query("SELECT j.user, COUNT(j) FROM JobEntity j WHERE j.status IN (:statuses) GROUP BY j.user")
    Set<UserJobAmountAggregateProjection> getUsersJobCount(
        @Param("statuses") @NotEmpty final Set<JobStatus> statuses
    );

    /**
     * Produce a sum of memory used of all jobs in the given states for each user.
     *
     * @param statuses the job statuses to filter by
     * @return a set of {@link UserJobAmountAggregateProjection}
     */
    @Query("SELECT j.user, SUM(j.memoryUsed) FROM JobEntity j WHERE j.status IN (:statuses) GROUP BY j.user")
    Set<UserJobAmountAggregateProjection> getUsersJobsTotalMemory(
        @Param("statuses") @NotEmpty final Set<JobStatus> statuses
    );

    /**
     * Returns the slice of ids for jobs created before the given date with the given statuses.
     *
     * @param date     The date before which the job requests were created
     * @param statuses The set of statuses a job must be in in order to be returned
     * @param pageable The page of data to get
     * @return List of job request ids
     */
    Slice<IdProjection> findByCreatedBeforeAndStatusIn(
        @NotNull final Date date,
        @NotEmpty final Set<JobStatus> statuses,
        @NotNull Pageable pageable
    );
}
