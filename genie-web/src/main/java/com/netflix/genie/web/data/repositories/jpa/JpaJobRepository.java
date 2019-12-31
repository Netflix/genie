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
package com.netflix.genie.web.data.repositories.jpa;

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.web.data.entities.JobEntity;
import com.netflix.genie.web.data.entities.aggregates.UserJobResourcesAggregate;
import com.netflix.genie.web.data.entities.projections.AgentHostnameProjection;
import com.netflix.genie.web.data.entities.projections.IdProjection;
import com.netflix.genie.web.data.entities.projections.JobProjection;
import com.netflix.genie.web.data.entities.projections.UniqueIdProjection;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

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
    Set<JobProjection> findByAgentHostnameAndStatusIn(String agentHostname, Set<String> statuses);

    /**
     * Get the number of agent jobs on a given host with the given statuses.
     *
     * @param agentHostname The hostname of the agent
     * @param statuses      The statuses the job should be in e.g. {@link JobStatus#getActiveStatuses()}
     * @return The number of jobs in the given statuses on that node
     */
    long countByAgentHostnameAndStatusIn(String agentHostname, Set<String> statuses);

    /**
     * Given the hostname that agents are running on return the total memory their jobs are currently using.
     *
     * @param agentHostname The agent hostname
     * @param statuses      The job statuses to filter by e.g. {@link JobStatus#getActiveStatuses()}
     * @return The total memory used in MB
     */
    @Query(
        "SELECT COALESCE(SUM(j.memoryUsed), 0)"
            + " FROM JobEntity j"
            + " WHERE j.agentHostname = :agentHostname AND j.status IN (:statuses)"
    )
    long getTotalMemoryUsedOnHost(
        @Param("agentHostname") String agentHostname,
        @Param("statuses") Set<String> statuses
    );

    /**
     * Find the jobs with one of the statuses entered.
     *
     * @param statuses The statuses to search
     * @return The job information requested
     */
    Set<AgentHostnameProjection> findDistinctByStatusInAndV4IsFalse(Set<String> statuses);

    /**
     * Deletes all jobs for the given ids.
     *
     * @param ids list of ids for which the jobs should be deleted
     * @return no. of jobs deleted
     */
    @Modifying
    Long deleteByIdIn(@NotNull List<Long> ids);

    /**
     * Count all jobs that belong to a given user and are in any of the given states.
     *
     * @param user     the user name
     * @param statuses the set of statuses
     * @return the count of jobs matching the search criteria
     */
    Long countJobsByUserAndStatusIn(@NotBlank String user, @NotEmpty Set<String> statuses);

    /**
     * Returns the slice of ids for job requests created before the given date.
     *
     * @param date     The date before which the job requests were created
     * @param pageable The page of data to get
     * @return List of job request ids
     */
    // TODO: Explore deleteFirst{N}ByCreatedBefore
    Slice<IdProjection> findByCreatedBefore(@NotNull Instant date, @NotNull Pageable pageable);

    /**
     * Returns resources usage for each user that has a running job.
     * Only jobs running on Genie servers are considered (i.e. no Agent jobs)
     *
     * @return The user resource aggregates
     */
    @Query(
        "SELECT j.user AS user, COUNT(j) as runningJobsCount, SUM(j.memoryUsed) as usedMemory"
            + " FROM JobEntity j"
            + " WHERE j.status = 'RUNNING' AND j.v4 = FALSE"
            + " GROUP BY j.user"
    )
    Set<UserJobResourcesAggregate> getUserJobResourcesAggregates();

    /**
     * Find agent jobs in the given set of states that don't have an entry in the connections table.
     *
     * @param statuses the job statuses filter
     * @return a set of job projections
     */
    @Query(
        "SELECT j"
            + " FROM JobEntity j"
            + " WHERE j.status IN (:statuses)"
            + " AND j.v4 = TRUE"
            + " AND j.uniqueId NOT IN (SELECT c.jobId FROM AgentConnectionEntity c)"
    )
    Set<UniqueIdProjection> getAgentJobIdsWithNoConnectionInState(@Param("statuses") @NotEmpty Set<String> statuses);
}
