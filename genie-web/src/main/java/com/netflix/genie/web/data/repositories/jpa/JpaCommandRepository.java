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

import com.netflix.genie.web.data.entities.CommandEntity;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Set;

/**
 * Command repository.
 *
 * @author tgianos
 */
public interface JpaCommandRepository extends JpaBaseRepository<CommandEntity> {

    /**
     * The query used to set commands to a given status given input parameters.
     */
    String SET_UNUSED_STATUS_QUERY =
        "UPDATE commands"
            + " SET status = :desiredStatus"
            + " WHERE status IN (:currentStatuses)"
            + " AND created < :commandCreatedThreshold"
            + " AND id NOT IN ("
            + "SELECT DISTINCT(command_id)"
            + " FROM jobs"
            + " WHERE command_id IS NOT NULL"
            + " AND created >= :jobCreatedThreshold"
            + ");";

    /**
     * Bulk set the status of commands which match the given inputs. Considers whether a command was used in some
     * period of time till now.
     *
     * @param desiredStatus           The new status the matching commands should have
     * @param commandCreatedThreshold The instant in time which a command must have been created before to be
     *                                considered for update. Exclusive
     * @param currentStatuses         The set of current statuses a command must have to be considered for update
     * @param jobCreatedThreshold     The instant in time after which a command must not have been used in a Genie job
     *                                for it to be considered for update. Inclusive.
     * @return The number of commands that were updated by the query
     */
    @Query(value = SET_UNUSED_STATUS_QUERY, nativeQuery = true)
    @Modifying
    int setUnusedStatus(
        @Param("desiredStatus") String desiredStatus,
        @Param("commandCreatedThreshold") Instant commandCreatedThreshold,
        @Param("currentStatuses") Set<String> currentStatuses,
        @Param("jobCreatedThreshold") Instant jobCreatedThreshold
    );
}
