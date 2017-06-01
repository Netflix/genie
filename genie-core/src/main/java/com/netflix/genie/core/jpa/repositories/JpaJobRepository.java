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
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;

/**
 * Job repository.
 *
 * @author tgianos
 */
public interface JpaJobRepository extends JpaRepository<JobEntity, String>, JpaSpecificationExecutor {
    /**
     * Deletes all jobs for the given ids.
     *
     * @param ids list of ids for which the jobs should be deleted
     * @return no. of jobs deleted
     */
    Long deleteByIdIn(@NotNull final List<String> ids);

    /**
     * Count all jobs that belong to a given user and are in any of the given states.
     *
     * @param user the user name
     * @param statuses the set of statuses
     * @return the count of jobs matching the search criteria
     */
    Long countJobsByUserAndStatusIn(@NotBlank final String user, @NotEmpty final Set<JobStatus> statuses);
}
