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
package com.netflix.genie.server.resources;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Types.ClusterStatus;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code for ClusterConfigResourceV1 - REST end-point for supporting cluster
 * configurations.
 *
 * @author amsharma
 * @author tgianos
 */
@Path("/v1/config/clusters")
@Api(value = "/v1/config/clusters", description = "Manage the available clusters")
@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
public class ClusterConfigResourceV1 {

    private static final Logger LOG = LoggerFactory
            .getLogger(ClusterConfigResourceV1.class);

    /**
     * The cluster service.
     */
    private final ClusterConfigService ccs;

    /**
     * Uri info for gathering information on the request.
     */
    @Context
    private UriInfo uriInfo;

    /**
     * Default constructor.
     *
     * @throws CloudServiceException if there is any error
     */
    public ClusterConfigResourceV1() throws CloudServiceException {
        this.ccs = ConfigServiceFactory.getClusterConfigImpl();
    }

    /**
     * Get cluster configuration from unique id.
     *
     * @param id id for the cluster
     * @return the cluster
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}")
    @ApiOperation(
            value = "Find a cluster by id",
            notes = "Get the cluster by id if it exists",
            response = Cluster.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Cluster.class),
        @ApiResponse(code = 400, message = "Invalid id supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public Cluster getCluster(
            @ApiParam(value = "Id of the cluster to get.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("called with id: " + id);
        return this.ccs.getCluster(id);
    }

    /**
     * Get cluster config based on user params. If empty strings are passed for
     * they are treated as nulls (not false).
     *
     * @param name cluster name (can be a pattern)
     * @param statuses valid types - Types.ClusterStatus
     * @param tags tags for the cluster
     * @param minUpdateTime min time when cluster configuration was updated
     * @param maxUpdateTime max time when cluster configuration was updated
     * @param limit number of entries to return
     * @param page page number
     * @return the Clusters found matching the criteria
     * @throws CloudServiceException
     */
    @GET
    @ApiOperation(
            value = "Find clusters",
            notes = "Find clusters by the submitted criteria.",
            response = Cluster.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Cluster.class)
    })
    public List<Cluster> getClusters(
            @ApiParam(value = "Name of the cluster.", required = false)
            @QueryParam("name")
            final String name,
            @ApiParam(value = "Status of the cluster.", required = false)
            @QueryParam("status")
            final List<String> statuses,
            @ApiParam(value = "Tags for the cluster.", required = false)
            @QueryParam("tag")
            final List<String> tags,
            @ApiParam(value = "Minimum time threshold for cluster update", required = false)
            @QueryParam("minUpdateTime")
            final Long minUpdateTime,
            @ApiParam(value = "Maximum time threshold for cluster update", required = false)
            @QueryParam("maxUpdateTime")
            final Long maxUpdateTime,
            @ApiParam(value = "The page to start on.", required = false)
            @QueryParam("page")
            @DefaultValue("0") int page,
            @ApiParam(value = "Max number of results per page.", required = false)
            @QueryParam("limit")
            @DefaultValue("1024") int limit)
            throws CloudServiceException {
        LOG.debug("called");
        //Create this conversion internal in case someone uses lower case by accident?
        List<ClusterStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = new ArrayList<ClusterStatus>();
            for (final String status : statuses) {
                enumStatuses.add(ClusterStatus.parse(status));
            }
        }
        return this.ccs.getClusters(name, enumStatuses, tags, minUpdateTime, maxUpdateTime, limit, page);
    }

    /**
     * Create cluster configuration.
     *
     * @param cluster contains the cluster information to create
     * @return The created cluster
     * @throws CloudServiceException
     */
    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @ApiOperation(
            value = "Create a cluster",
            notes = "Create a cluster from the supplied information.",
            response = Cluster.class)
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created", response = Cluster.class),
        @ApiResponse(code = 400, message = "Invalid required parameter supplied"),
        @ApiResponse(code = 409, message = "A cluster with the supplied id already exists")
    })
    public Cluster createCluster(
            @ApiParam(value = "The cluster to create.", required = true)
            final Cluster cluster)
                    throws CloudServiceException {
        LOG.debug("called to create new cluster");
        return this.ccs.createCluster(cluster);
    }

    /**
     * Update a cluster configuration.
     *
     * @param id unique if for cluster to update
     * @param updateCluster contains the cluster information to update
     * @return the updated cluster
     * @throws CloudServiceException
     */
    @PUT
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @ApiOperation(
            value = "Update a cluster",
            notes = "Update a cluster from the supplied information.",
            response = Cluster.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Cluster.class),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Cluster to update not found")
    })
    public Cluster updateClusterConfig(
            @ApiParam(value = "Id of the cluster to update.", required = true)
            @PathParam("id")
            final String id,
                final Cluster updateCluster) throws CloudServiceException {
        LOG.debug("called to create/update cluster");
        return this.ccs.updateCluster(id, updateCluster);
    }

    /**
     * Delete a cluster configuration.
     *
     * @param id unique id for cluster to delete
     * @return the deleted cluster
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}")
    @ApiOperation(
            value = "Delete a cluster",
            notes = "Delete a cluster with the supplied id.",
            response = Cluster.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Cluster.class),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public Cluster deleteClusterConfig(@PathParam("id") final String id) throws CloudServiceException {
        LOG.debug("delete called for id: " + id);
        return this.ccs.deleteCluster(id);
    }

    /**
     * Delete all clusters from database.
     *
     * @return All The deleted clusters
     * @throws CloudServiceException
     */
    @DELETE
    @ApiOperation(
            value = "Delete all clusters",
            notes = "Delete all available clusters and get them back.",
            response = Cluster.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public List<Cluster> deleteAllClusters() throws CloudServiceException {
        LOG.debug("called");
        return this.ccs.deleteAllClusters();
    }

    /**
     * Add new configuration files to a given cluster.
     *
     * @param id The id of the cluster to add the configuration file to. Not
     * null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @return The active configurations for this cluster.
     * @throws CloudServiceException
     */
    @POST
    @Path("/{id}/configs")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new configuration files to a cluster",
            notes = "Add the supplied configuration files to the cluster with the supplied id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public Set<String> addConfigsForCluster(
            @ApiParam(value = "Id of the cluster to add configuration to.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The configuration files to add.", required = true)
            final Set<String> configs) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and config " + configs);
        return this.ccs.addConfigsForCluster(id, configs);
    }

    /**
     * Get all the configuration files for a given cluster.
     *
     * @param id The id of the cluster to get the configuration files for. Not
     * NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}/configs")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get the configuration files for a cluster",
            notes = "Get the configuration files for the cluster with the supplied id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public Set<String> getConfigsForCluster(
            @ApiParam(value = "Id of the cluster to get configurations for.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called with id " + id);
        return this.ccs.getConfigsForCluster(id);
    }

    /**
     * Update the configuration files for a given cluster.
     *
     * @param id The id of the cluster to update the configuration files for.
     * Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     * files with. Not null/empty/blank.
     * @return The new set of cluster configurations.
     * @throws CloudServiceException
     */
    @PUT
    @Path("/{id}/configs")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update configuration files for an cluster",
            notes = "Replace the existing configuration files for cluster with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public Set<String> updateConfigsForCluster(
            @ApiParam(value = "Id of the cluster to update configurations for.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The configuration files to replace existing with.", required = true)
            final Set<String> configs) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and configs " + configs);
        return this.ccs.updateConfigsForCluster(id, configs);
    }

    /**
     * Delete the all configuration files from a given cluster.
     *
     * @param id The id of the cluster to delete the configuration files from.
     * Not null/empty/blank.
     * @return Empty set if successful
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}/configs")
    @ApiOperation(
            value = "Remove all configuration files from a cluster",
            notes = "Remove all the configuration files from the cluster with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public Set<String> removeAllConfigsForCluster(
            @ApiParam(value = "Id of the cluster to delete from.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called with id " + id);
        return this.ccs.removeAllConfigsForCluster(id);
    }

    /**
     * Delete the configuration file from a given cluster.
     *
     * @param id The id of the cluster to delete the configuration file from.
     * Not null/empty/blank.
     * @param config The configuration file to remove.
     * @return The active set of cluster configurations.
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}/configs/{config}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Remove a configuration file from an cluster",
            notes = "Remove the given configuration file from the cluster with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "cluster not found")
    })
    public Set<String> removeConfigForCluster(
            @ApiParam(value = "Id of the cluster to delete from.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The configuration file to delete.", required = true)
            @PathParam("config")
            final String config) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and config " + config);
        return this.ccs.removeConfigForCluster(id, config);
    }

    /**
     * Add new commands to the given cluster.
     *
     * @param id The id of the cluster to add the commands to. Not
     * null/empty/blank.
     * @param commands The commands to add. Not null.
     * @return The active commands for this cluster.
     * @throws CloudServiceException
     */
    @POST
    @Path("/{id}/commands")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @ApiOperation(
            value = "Add new commands to a cluster",
            notes = "Add the supplied commands to the cluster with the supplied id."
            + " commands should already have been created.",
            response = Command.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "cluster not found")
    })
    public List<Command> addCommandsForcluster(
            @ApiParam(value = "Id of the cluster to add commands to.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The commands to add.", required = true)
            final List<Command> commands) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and commands " + commands);
        return this.ccs.addCommandsForCluster(id, commands);
    }

    /**
     * Get all the commands configured for a given cluster.
     *
     * @param id The id of the cluster to get the command files for. Not
     * NULL/empty/blank.
     * @return The active set of commands for the cluster.
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}/commands")
    @ApiOperation(
            value = "Get the commands for a cluster",
            notes = "Get the commands for the cluster with the supplied id.",
            response = Command.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public List<Command> getCommandsForCluster(
            @ApiParam(value = "Id of the cluster to get commands for.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called with id " + id);
        return this.ccs.getCommandsForCluster(id);
    }

    /**
     * Update the commands for a given cluster.
     *
     * @param id The id of the cluster to update the configuration files for.
     * Not null/empty/blank.
     * @param commands The commands to replace existing applications
     * with. Not null/empty/blank.
     * @return The new set of commands for the cluster.
     * @throws CloudServiceException
     */
    @PUT
    @Path("/{id}/commands")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @ApiOperation(
            value = "Update the commands for an cluster",
            notes = "Replace the existing commands for cluster with given id.",
            response = Command.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public List<Command> updateCommandsForCluster(
            @ApiParam(value = "Id of the cluster to update commands for.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The commands to replace existing with. Should already be created",
                    required = true)
            final List<Command> commands) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and configs " + commands);
        return this.ccs.updateCommandsForCluster(id, commands);
    }

    /**
     * Remove the all commands from a given cluster.
     *
     * @param id The id of the cluster to delete the commands from. Not
     * null/empty/blank.
     * @return Empty set if successful
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}/commands")
    @ApiOperation(
            value = "Remove all commands from an cluster",
            notes = "Remove all the commands from the cluster with given id.",
            response = Command.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public List<Command> removeAllCommandsForCluster(
            @ApiParam(value = "Id of the cluster to delete from.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called with id " + id);
        return this.ccs.removeAllCommandsForCluster(id);
    }

    /**
     * Remove an command from a given cluster.
     *
     * @param id The id of the cluster to delete the command from. Not
     * null/empty/blank.
     * @param cmdId The id of the command to remove. Not null/empty/blank.
     * @return The active set of commands for the cluster.
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}/commands/{cmdId}")
    @ApiOperation(
            value = "Remove a command from a cluster",
            notes = "Remove the given command from the cluster with given id.",
            response = Command.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Cluster not found")
    })
    public List<Command> removeCommandForCluster(
            @ApiParam(value = "Id of the cluster to delete from.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The id of the command to remove.", required = true)
            @PathParam("appId")
            final String cmdId) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and command id " + cmdId);
        return this.ccs.removeCommandForCluster(id, cmdId);
    }
}
