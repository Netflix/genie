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
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import javax.validation.constraints.NotNull;
import java.util.Date;

/**
 * Job repository.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Repository
public interface JpaJobRequestRepository extends JpaRepository<JobRequestEntity, String>, JpaSpecificationExecutor {

    /**
     * Delete all job requests created before the given date.
     *
     * @param date The date before which all job requests should be deleted.
     * @return The number of deleted records
     */
    Long deleteByCreatedBefore(@NotNull final Date date);
}
