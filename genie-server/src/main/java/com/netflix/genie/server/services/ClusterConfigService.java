/*
 *
 *  Copyright 2014 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.server.services;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.ClusterStatus;

import java.util.List;
import java.util.Set;

/**
 * Abstraction layer to encapsulate data ClusterConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
public interface ClusterConfigService {

    /**
     * Create new cluster configuration.
     *
     * @param cluster The cluster to create
     * @return The created cluster
     * @throws GenieException if there is an error
     */
    Cluster createCluster(final Cluster cluster) throws GenieException;

    /**
     * Get the cluster configuration by id.
     *
     * @param id unique id of cluster configuration to return
     * @return The cluster configuration
     * @throws GenieException For any error
     */
    Cluster getCluster(final String id) throws GenieException;

    /**
     * Get cluster info for various parameters. Null or empty parameters are
     * ignored.
     *
     * @param name          cluster name
     * @param statuses      valid types - Types.ClusterStatus
     * @param tags          tags allocated to this cluster
     * @param minUpdateTime min time when cluster configuration was updated
     * @param maxUpdateTime max time when cluster configuration was updated
     * @param page          page number
     * @param limit         number of entries to return
     * @return All the clusters matching the criteria
     */
    List<Cluster> getClusters(
            final String name,
            final Set<ClusterStatus> statuses,
            final Set<String> tags,
            final Long minUpdateTime,
            final Long maxUpdateTime,
            final int page,
            final int limit);

    /**
     * Get the clusters on which the job can be run.
     *
     * @param jobId The id of the Job to run
     * @return successful response, or one with HTTP error code
     * @throws GenieException if there is an error
     */
    List<Cluster> chooseClusterForJob(final String jobId) throws GenieException;

    /**
     * Update a cluster configuration.
     *
     * @param id            The id of the cluster to update
     * @param updateCluster the information to update the cluster with
     * @return the updated cluster
     * @throws GenieException if there is an error
     */
    Cluster updateCluster(
            final String id,
            final Cluster updateCluster) throws GenieException;

    /**
     * Delete all clusters from database.
     *
     * @return The deleted clusters
     * @throws GenieException if there is an error
     */
    List<Cluster> deleteAllClusters() throws GenieException;

    /**
     * Delete a cluster configuration by id.
     *
     * @param id unique id for cluster to delete
     * @return the deleted cluster
     * @throws GenieException if there is an error
     */
    Cluster deleteCluster(final String id) throws GenieException;

    /**
     * Add configuration files to the cluster.
     *
     * @param id      The id of the cluster to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> addConfigsForCluster(
            final String id,
            final Set<String> configs) throws GenieException;

    /**
     * Get the set of configuration files associated with the cluster with given
     * id.
     *
     * @param id The id of the cluster to get the configuration files for. Not
     *           null/empty/blank.
     * @return The set of configuration files as paths
     * @throws GenieException if there is an error
     */
    Set<String> getConfigsForCluster(
            final String id) throws GenieException;

    /**
     * Update the set of configuration files associated with the cluster with
     * given id.
     *
     * @param id      The id of the cluster to update the configuration files for.
     *                Not null/empty/blank.
     * @param configs The configuration files to replace existing configurations
     *                with. Not null/empty.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> updateConfigsForCluster(
            final String id,
            final Set<String> configs) throws GenieException;

    /**
     * Add commands to the cluster.
     *
     * @param id       The id of the cluster to add the command file to. Not
     *                 null/empty/blank.
     * @param commands The commands to add. Not null/empty.
     * @return The active list of commands
     * @throws GenieException if there is an error
     */
    List<Command> addCommandsForCluster(
            final String id,
            final List<Command> commands) throws GenieException;

    /**
     * Get the set of commands associated with the cluster with given id.
     *
     * @param id The id of the cluster to get the commands for. Not
     *           null/empty/blank.
     * @return The list of commands
     * @throws GenieException if there is an error
     */
    List<Command> getCommandsForCluster(final String id) throws GenieException;

    /**
     * Update the set of command files associated with the cluster with
     * given id.
     *
     * @param id       The id of the cluster to update the command files for. Not
     *                 null/empty/blank.
     * @param commands The command files to replace existing
     *                 commands with. Not null/empty.
     * @return The active list of commands
     * @throws GenieException if there is an error
     */
    List<Command> updateCommandsForCluster(
            final String id,
            final List<Command> commands) throws GenieException;

    /**
     * Remove all commands from the cluster.
     *
     * @param id The id of the cluster to remove the commands from. Not
     *           null/empty/blank.
     * @return The active list of commands
     * @throws GenieException if there is an error
     */
    List<Command> removeAllCommandsForCluster(
            final String id) throws GenieException;

    /**
     * Remove a command from the cluster.
     *
     * @param id    The id of the cluster to remove the command from. Not
     *              null/empty/blank.
     * @param cmdId The id of the command to remove. Not null/empty/blank.
     * @return The active list of commands
     * @throws GenieException if there is an error
     */
    List<Command> removeCommandForCluster(final String id, final String cmdId) throws GenieException;

    /**
     * Add tags to the cluster.
     *
     * @param id   The id of the cluster to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> addTagsForCluster(
            final String id,
            final Set<String> tags) throws GenieException;

    /**
     * Get the set of tags associated with the cluster with given
     * id.
     *
     * @param id The id of the cluster to get the tags for. Not
     *           null/empty/blank.
     * @return The set of tags as paths
     * @throws GenieException if there is an error
     */
    Set<String> getTagsForCluster(
            final String id) throws GenieException;

    /**
     * Update the set of tags associated with the cluster with
     * given id.
     *
     * @param id   The id of the cluster to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing tags
     *             with. Not null/empty.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> updateTagsForCluster(
            final String id,
            final Set<String> tags) throws GenieException;

    /**
     * Remove all tags from the cluster.
     *
     * @param id The id of the cluster to remove the tags from.
     *           Not null/empty/blank.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> removeAllTagsForCluster(
            final String id) throws GenieException;

    /**
     * Remove a tag from the cluster.
     *
     * @param id  The id of the cluster to remove the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> removeTagForCluster(final String id, final String tag) throws GenieException;
}
