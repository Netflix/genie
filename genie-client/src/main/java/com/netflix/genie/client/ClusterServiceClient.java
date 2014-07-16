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
package com.netflix.genie.client;

import com.google.common.collect.Multimap;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton class, which acts as the client library for the Cluster
 * Configuration Service.
 *
 * @author skrishnan
 * @author tgianos
 * @author amsharma
 */
// TODO: Can probably templetize the config clients or part of them as
//     much of the code is the same
public final class ClusterServiceClient extends BaseGenieClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(ClusterServiceClient.class);

    private static final String BASE_CONFIG_CLUSTER_REST_URL
            = BASE_REST_URL + "config/clusters";

    // reference to the instance object
    private static ClusterServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private ClusterServiceClient() throws IOException {
        super();
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ExecutionServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized ClusterServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new ClusterServiceClient();
        }

        return instance;
    }

    /**
     * Create a new cluster configuration.
     *
     * @param cluster the object encapsulating the new Cluster configuration to
     * create
     *
     * @return extracted cluster configuration response
     * @throws GenieException
     */
    public Cluster createCluster(final Cluster cluster)
            throws GenieException {
        if (cluster == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster entered. Unable to validate.");
        }
        cluster.validate();
        final HttpRequest request = this.buildRequest(
                Verb.POST,
                BASE_CONFIG_CLUSTER_REST_URL,
                null,
                cluster);
        return (Cluster) this.executeRequest(request, null, Cluster.class);
    }

    /**
     * Create or update a cluster configuration.
     *
     * @param id the id for the cluster configuration to create or update
     * @param cluster the object encapsulating the new Cluster configuration to
     * create
     *
     * @return extracted cluster configuration response
     * @throws GenieException
     */
    public Cluster updateCluster(
            final String id,
            final Cluster cluster)
            throws GenieException {
        if (StringUtils.isBlank(id)) {
            String msg = "Required parameter id can't be null or empty.";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id},
                        SLASH),
                null,
                cluster);
        return (Cluster) this.executeRequest(request, null, Cluster.class);
    }

    /**
     * Gets information for a given id.
     *
     * @param id the cluster configuration id to get (can't be null or empty)
     * @return the cluster configuration for this id
     * @throws GenieException
     */
    public Cluster getCluster(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id},
                        SLASH),
                null,
                null);
        return (Cluster) this.executeRequest(request, null, Cluster.class);
    }

    /**
     * Gets a set of cluster configurations for the given parameters.
     *
     * @param params key/value pairs in a map object.<br>
     *
     * More details on the parameters can be found on the Genie User Guide on
     * GitHub.
     * @return List of cluster configuration elements that match the filter
     * @throws GenieException
     */
    public List<Cluster> getClusters(final Multimap<String, String> params)
            throws GenieException {
        final HttpRequest request = this.buildRequest(
                Verb.GET,
                BASE_CONFIG_CLUSTER_REST_URL,
                params,
                null);
        return (List<Cluster>) this.executeRequest(request, List.class, Cluster.class);
    }

    /**
     * Delete all the clusters in the database.
     *
     * @return the should be empty set.
     * @throws GenieException
     */
    public List<Cluster> deleteAllClusters() throws GenieException {
        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                BASE_CONFIG_CLUSTER_REST_URL,
                null,
                null);
        return (List<Cluster>) this.executeRequest(request, List.class, Cluster.class);
    }

    /**
     * Delete a cluster using its id.
     *
     * @param id the id for the cluster cluster to delete
     * @return the deleted cluster cluster
     * @throws GenieException
     */
    public Cluster deleteCluster(final String id) throws GenieException {
        if (StringUtils.isEmpty(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id},
                        SLASH),
                null,
                null);
        return (Cluster) this.executeRequest(request, null, Cluster.class);
    }

    /**
     * Add some more configuration files to a given cluster.
     *
     * @param id The id of the cluster to add configurations to. Not
     * Null/empty/blank.
     * @param configs The configuration files to add. Not null or empty.
     * @return The new set of configuration files for the given command.
     * @throws GenieException
     */
    public Set<String> addConfigsToCluster(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (configs == null || configs.isEmpty()) {
            final String msg = "Missing required parameter: configs";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.POST,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "configs"},
                        SLASH),
                null,
                configs);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Get the active set of configuration files for the given cluster.
     *
     * @param id The id of the cluster to get configurations for. Not
     * Null/empty/blank.
     * @return The set of configuration files for the given cluster.
     * @throws GenieException
     */
    public Set<String> getConfigsForCluster(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "configs"},
                        SLASH),
                null,
                null);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Update the configuration files for a given cluster.
     *
     * @param id The id of the cluster to update the configuration files for.
     * Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     * files with. Not null.
     * @return The new set of cluster configurations.
     * @throws GenieException
     */
    public Set<String> updateConfigsForCluster(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (configs == null) {
            final String msg = "Missing required parameter: configs";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "configs"},
                        SLASH),
                null,
                configs);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Delete all the configuration files from a given cluster.
     *
     * @param id The id of the cluster to delete the configuration files from.
     * Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException
     */
    public Set<String> removeAllConfigsForCluster(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "configs"},
                        SLASH),
                null,
                null);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Add some more commands to a given cluster.
     *
     * @param id The id of the cluster to add commands to. Not
     * Null/empty/blank.
     * @param commands The commands to add. Not null or empty.
     * @return The new list of commands for the given cluster.
     * @throws GenieException
     */
    public List<Command> addCommandsToCluster(
            final String id,
            final List<Command> commands) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (commands == null || commands.isEmpty()) {
            final String msg = "Missing required parameter: commands";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.POST,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "commands"},
                        SLASH),
                null,
                commands);
        return (List<Command>) this.executeRequest(request, List.class, Command.class);
    }

    /**
     * Get the active set of commands for the given cluster.
     *
     * @param id The id of the cluster to get commands for. Not
     * Null/empty/blank.
     * @return The list of command files for the given cluster.
     * @throws GenieException
     */
    public List<Command> getCommandsForCluster(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "commands"},
                        SLASH),
                null,
                null);
        return (List<Command>) this.executeRequest(request, List.class, Command.class);
    }

    /**
     * Update the commands for a given cluster.
     *
     * @param id The id of the cluster to update the command files for. Not
     * null/empty/blank.
     * @param commands The commands to replace existing command
     * files with. Not null.
     * @return The new list of cluster commands.
     * @throws GenieException
     */
    public List<Command> updateCommandsForCluster(
            final String id,
            final List<Command> commands) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (commands == null) {
            final String msg = "Missing required parameter: commands";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "commands"},
                        SLASH),
                null,
                commands);
        return (List<Command>) this.executeRequest(request, List.class, Command.class);
    }

    /**
     * Delete all the commands from a given cluster.
     *
     * @param id The id of the cluster to delete the commands from. Not
     * null/empty/blank.
     * @return Empty list if successful
     * @throws GenieException
     */
    public List<Command> removeAllCommandsForCluster(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "commands"},
                        SLASH),
                null,
                null);
        return (List<Command>) this.executeRequest(request, List.class, Command.class);
    }

    /**
     * Remove an command from a given cluster.
     *
     * @param id The id of the cluster to delete the command from. Not
     * null/empty/blank.
     * @param cmdId The id of the command to remove. Not null/empty/blank.
     * @return The active set of commands for the cluster.
     * @throws GenieException
     */
    public Set<Command> removeCommandForCluster(
            final String id,
            final String cmdId) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (StringUtils.isBlank(cmdId)) {
            final String msg = "Missing required parameter: cmdId";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "commands", cmdId},
                        SLASH),
                null,
                null);
        return (Set<Command>) this.executeRequest(request, Set.class, Command.class);
    }

    /**
     * Add some more tags to a given cluster.
     *
     * @param id The id of the cluster to add tags to. Not
     * Null/empty/blank.
     * @param tags The tags to add. Not null or empty.
     * @return The new set of tags for the given cluster.
     * @throws GenieException
     */
    public Set<String> addTagsToCluster(
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (tags == null || tags.isEmpty()) {
            final String msg = "Missing required parameter: tags";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.POST,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "tags"},
                        SLASH),
                null,
                tags);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Get the active set of tags for the given cluster.
     *
     * @param id The id of the cluster to get tags for. Not
     * Null/empty/blank.
     * @return The set of tags for the given cluster.
     * @throws GenieException
     */
    public Set<String> getTagsForCluster(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "tags"},
                        SLASH),
                null,
                null);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Update the tags for a given cluster.
     *
     * @param id The id of the cluster to update the tags for.
     * Not null/empty/blank.
     * @param tags The tags to replace existing tag
     * files with. Not null.
     * @return The new set of cluster tags.
     * @throws GenieException
     */
    public Set<String> updateTagsForCluster(
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (tags == null) {
            final String msg = "Missing required parameter: tags";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "tags"},
                        SLASH),
                null,
                tags);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Delete all the tags from a given cluster.
     *
     * @param id The id of the cluster to delete the tags from.
     * Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException
     */
    public Set<String> removeAllTagsForCluster(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_CLUSTER_REST_URL, id, "tags"},
                        SLASH),
                null,
                null);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }
    
    /**
     * Remove tag from a given cluster.
     *
     * @param id The id of the cluster to delete the tag from. Not
     * null/empty/blank.
     * @return The tag for the cluster.
     * @throws GenieException
     */
    public Set<String> removeTagForCluster(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{
                            BASE_CONFIG_CLUSTER_REST_URL,
                            id,
                            "tags"
                        },
                        SLASH),
                null,
                null);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }
}
