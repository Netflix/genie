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
            + ")";

    /**
     * The query used to find commands that are unused to delete.
     */
    String FIND_UNUSED_COMMANDS_QUERY =
        "SELECT id"
            + " FROM commands"
            + " WHERE status IN (:unusedStatuses)"
            + " AND created < :commandCreatedThreshold"
            + " AND id NOT IN ("
            + "SELECT DISTINCT(command_id)"
            + " FROM jobs"
            + " WHERE command_id IS NOT NULL"
            + ")"
            + " AND id NOT IN (SELECT DISTINCT(command_id) FROM clusters_commands)";

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

    /**
     * Find commands from the database where their status is in {@literal deleteStatuses} they were created
     * before {@literal commandCreatedThreshold} and they aren't attached to any jobs still in the database.
     *
     * @param unusedStatuses          The set of statuses a command must be in in order to be considered unused
     * @param commandCreatedThreshold The instant in time a command must have been created before to be considered
     *                                unused. Exclusive.
     * @return The ids of the commands that are considered unused
     */
    @Query(value = FIND_UNUSED_COMMANDS_QUERY, nativeQuery = true)
    Set<Long> findUnusedCommands(
        @Param("unusedStatuses") Set<String> unusedStatuses,
        @Param("commandCreatedThreshold") Instant commandCreatedThreshold
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
