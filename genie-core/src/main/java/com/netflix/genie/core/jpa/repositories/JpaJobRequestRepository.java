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

import com.netflix.genie.core.jpa.entities.JobRequestEntity;
import com.netflix.genie.core.jpa.entities.projections.IdProjection;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;

/**
 * Job repository.
 *
 * @author tgianos
 * @since 3.0.0
 */
public interface JpaJobRequestRepository extends JpaRepository<JobRequestEntity, String>, JpaSpecificationExecutor {

    /**
     * Returns the slice of ids for job requests created before the given date.
     *
     * @param date     The date before which the job requests were created
     * @param pageable The page of data to get
     * @return List of job request ids
     */
    Slice<IdProjection> findByCreatedBefore(@NotNull final Date date, @NotNull Pageable pageable);

    /**
     * Deletes all job requests for the given ids.
     *
     * @param ids list of ids for which the job requests should be deleted
     * @return no. of requests deleted
     */
    Long deleteByIdIn(@NotNull final List<String> ids);
}
