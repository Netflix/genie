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
package com.netflix.genie.web.jpa.repositories;

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.web.jpa.entities.JobEntity;
import com.netflix.genie.web.jpa.entities.projections.AgentHostnameProjection;
import com.netflix.genie.web.jpa.entities.projections.IdProjection;
import com.netflix.genie.web.jpa.entities.projections.JobProjection;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.Modifying;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.time.Instant;
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
     * @param agentHostname The host name to search for
     * @param statuses      The job statuses to filter by
     * @return The jobs
     */
    Set<JobProjection> findByAgentHostnameAndStatusIn(final String agentHostname, final Set<JobStatus> statuses);

    /**
     * Find the jobs with one of the statuses entered.
     *
     * @param statuses The statuses to search
     * @return The job information requested
     */
    Set<AgentHostnameProjection> findDistinctByStatusInAndV4IsFalse(final Set<JobStatus> statuses);

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
    Slice<IdProjection> findByCreatedBefore(@NotNull final Instant date, @NotNull Pageable pageable);
}
