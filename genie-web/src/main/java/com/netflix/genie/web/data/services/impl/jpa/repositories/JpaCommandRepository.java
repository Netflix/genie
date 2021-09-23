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

import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * Command repository.
 *
 * @author tgianos
 */
public interface JpaCommandRepository extends JpaBaseRepository<CommandEntity> {

    /**
     * The query used to find commands that are in a certain status, not used in jobs and created some time ago.
     */
    String FIND_UNUSED_COMMANDS_IN_STATUS_CREATED_BEFORE_QUERY =
        "SELECT id"
            + " FROM commands"
            + " WHERE status IN (:statuses)"
            + " AND created < :commandCreatedThreshold"
            + " AND id NOT IN ("
            + "SELECT DISTINCT(command_id)"
            + " FROM jobs"
            + " WHERE command_id IS NOT NULL"
            + ")"
            + " LIMIT :limit";

    /**
     * Bulk set the status of commands which match the given inputs.
     *
     * @param desiredStatus The new status the matching commands should have
     * @param commandIds    The ids which should be updated
     * @return The number of commands that were updated by the query
     */
    @Query(value = "UPDATE CommandEntity c SET c.status = :desiredStatus WHERE c.id IN (:commandIds)")
    @Modifying
    int setStatusWhereIdIn(
        @Param("desiredStatus") String desiredStatus,
        @Param("commandIds") Set<Long> commandIds
    );

    /**
     * Find commands from the database where their status is in {@literal statuses} and they were created
     * before {@literal commandCreatedThreshold} and they aren't attached to any jobs still in the database.
     *
     * @param statuses                The set of statuses a command must be in for it to be considered unused
     * @param commandCreatedThreshold The instant in time a command must have been created before to be considered
     *                                unused. Exclusive.
     * @param limit                   Maximum number of IDs to return
     * @return The ids of the commands that are considered unused
     */
    @Query(value = FIND_UNUSED_COMMANDS_IN_STATUS_CREATED_BEFORE_QUERY, nativeQuery = true)
    Set<Long> findUnusedCommandsByStatusesCreatedBefore(
        @Param("statuses") Set<String> statuses,
        @Param("commandCreatedThreshold") Instant commandCreatedThreshold,
        @Param("limit") int limit
    );

    /**
     * Find the command with the given id but also eagerly load that commands applications.
     *
     * @param id The id of the command to get
     * @return The {@link CommandEntity} with its applications data loaded or {@link Optional#empty()} if there is no
     * command with the given id
     */
    @Query("SELECT c FROM CommandEntity c WHERE c.uniqueId = :id")
    @EntityGraph(value = CommandEntity.APPLICATIONS_ENTITY_GRAPH, type = EntityGraph.EntityGraphType.LOAD)
    Optional<CommandEntity> getCommandAndApplications(@Param("id") String id);

    /**
     * Find the command with the given id but also eagerly load that commands applications full dto contents.
     *
     * @param id The id of the command to get
     * @return The {@link CommandEntity} with its applications data loaded or {@link Optional#empty()} if there is no
     * command with the given id
     */
    @Query("SELECT c FROM CommandEntity c WHERE c.uniqueId = :id")
    @EntityGraph(value = CommandEntity.APPLICATIONS_DTO_ENTITY_GRAPH, type = EntityGraph.EntityGraphType.LOAD)
    Optional<CommandEntity> getCommandAndApplicationsDto(@Param("id") String id);

    /**
     * Find the command with the given id but also eagerly load that commands cluster criteria.
     *
     * @param id The id of the command to get
     * @return The {@link CommandEntity} with its criteria data loaded or {@link Optional#empty()} if there is no
     * command with the given id
     */
    @Query("SELECT c FROM CommandEntity c WHERE c.uniqueId = :id")
    @EntityGraph(value = CommandEntity.CLUSTER_CRITERIA_ENTITY_GRAPH, type = EntityGraph.EntityGraphType.LOAD)
    Optional<CommandEntity> getCommandAndClusterCriteria(@Param("id") String id);

    /**
     * Find the command with the given id but also eagerly load all data needed for a command DTO.
     *
     * @param id The id of the command to get
     * @return The {@link CommandEntity} all DTO data loaded or {@link Optional#empty()} if there is no
     * command with the given id
     */
    @Query("SELECT c FROM CommandEntity c WHERE c.uniqueId = :id")
    @EntityGraph(value = CommandEntity.DTO_ENTITY_GRAPH, type = EntityGraph.EntityGraphType.LOAD)
    Optional<CommandEntity> getCommandDto(@Param("id") String id);
}
