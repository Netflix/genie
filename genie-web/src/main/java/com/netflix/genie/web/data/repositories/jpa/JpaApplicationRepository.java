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

import com.netflix.genie.web.data.entities.ApplicationEntity;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

/**
 * Application repository.
 *
 * @author tgianos
 */
public interface JpaApplicationRepository extends JpaBaseRepository<ApplicationEntity> {

    /**
     * Delete any application records where it's not linked to any jobs and it's not linked to any commands and was
     * created before the given time.
     */
    String DELETE_APPLICATIONS_QUERY =
        "DELETE FROM applications"
            + " WHERE created < :createdThreshold"
            + " AND id NOT IN (SELECT DISTINCT(application_id) FROM commands_applications)"
            + " AND id NOT IN (SELECT DISTINCT(application_id) FROM jobs_applications);";


    /**
     * Delete any application records where it's not linked to any jobs and it's not linked to any commands and was
     * created before the given time.
     *
     * @param createdThreshold The instant in time before which records should be considered for deletion. Exclusive.O
     * @return The number of applications deleted
     */
    @Query(value = DELETE_APPLICATIONS_QUERY, nativeQuery = true)
    @Modifying
    int deleteUnusedApplications(@Param("createdThreshold") Instant createdThreshold);
}
