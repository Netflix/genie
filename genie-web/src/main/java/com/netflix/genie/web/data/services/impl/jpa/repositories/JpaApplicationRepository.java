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
package com.netflix.genie.web.data.services.impl.jpa.repositories;

import com.netflix.genie.web.data.services.impl.jpa.entities.ApplicationEntity;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * Application repository.
 *
 * @author tgianos
 */
public interface JpaApplicationRepository extends JpaBaseRepository<ApplicationEntity> {

    /**
     * Find any application records where they are not linked to any jobs and it's not linked to any commands and was
     * created before the given time.
     */
    String FIND_UNUSED_APPLICATIONS_QUERY =
        "SELECT id"
            + " FROM applications"
            + " WHERE created < :createdThreshold"
            + " AND id NOT IN (SELECT DISTINCT(application_id) FROM commands_applications)"
            + " AND id NOT IN (SELECT DISTINCT(application_id) FROM jobs_applications)"
            + " LIMIT :limit";

    /**
     * Delete any application records where it's not linked to any jobs and it's not linked to any commands and was
     * created before the given time.
     *
     * @param createdThreshold The instant in time before which records should be considered for deletion. Exclusive.
     * @param limit            Maximum number of IDs to return
     * @return The ids of the applications that are unused
     */
    @Query(value = FIND_UNUSED_APPLICATIONS_QUERY, nativeQuery = true)
    Set<Long> findUnusedApplications(
        @Param("createdThreshold") Instant createdThreshold,
        @Param("limit") int limit
    );

    /**
     * Get the {@link ApplicationEntity} but eagerly fetch all relational information needed to construct a DTO.
     *
     * @param id The unique identifier of the application to get
     * @return An {@link ApplicationEntity} with dto data loaded or {@link Optional#empty()} if no application with the
     * given id exists
     */
    @Query("SELECT a FROM ApplicationEntity a WHERE a.uniqueId = :id")
    @EntityGraph(value = ApplicationEntity.DTO_ENTITY_GRAPH, type = EntityGraph.EntityGraphType.LOAD)
    Optional<ApplicationEntity> getApplicationDto(@Param("id") String id);

    /**
     * Get the {@link ApplicationEntity} but eagerly fetch all command base information as well.
     *
     * @param id The unique identifier of the application to get
     * @return An {@link ApplicationEntity} with command data loaded or {@link Optional#empty()} if no application with
     * the given id exists
     */
    @Query("SELECT a FROM ApplicationEntity a WHERE a.uniqueId = :id")
    @EntityGraph(value = ApplicationEntity.COMMANDS_ENTITY_GRAPH, type = EntityGraph.EntityGraphType.LOAD)
    Optional<ApplicationEntity> getApplicationAndCommands(@Param("id") String id);

    /**
     * Get the {@link ApplicationEntity} but eagerly fetch all command DTO information as well.
     *
     * @param id The unique identifier of the application to get
     * @return An {@link ApplicationEntity} with command data loaded or {@link Optional#empty()} if no application with
     * the given id exists
     */
    @Query("SELECT a FROM ApplicationEntity a WHERE a.uniqueId = :id")
    @EntityGraph(value = ApplicationEntity.COMMANDS_DTO_ENTITY_GRAPH, type = EntityGraph.EntityGraphType.LOAD)
    Optional<ApplicationEntity> getApplicationAndCommandsDto(@Param("id") String id);
}
