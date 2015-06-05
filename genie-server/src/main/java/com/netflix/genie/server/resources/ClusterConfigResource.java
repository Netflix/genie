/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterStatus;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.server.services.ClusterConfigService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.HttpURLConnection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Code for ClusterConfigResource - REST end-point for supporting cluster
 * configurations.
 *
 * @author amsharma
 * @author tgianos
 */
@Named
@Path("/v2/config/clusters")
@Api(
        value = "/v2/config/clusters",
        tags = "clusters",
        description = "Manage the available clusters"
)
@Produces(MediaType.APPLICATION_JSON)
public final class ClusterConfigResource {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterConfigResource.class);

    /**
     * The Cluster Configuration Service.
     */
    private final ClusterConfigService clusterConfigService;

    /**
     * To get URI information for return codes.
     */
    @Context
    private UriInfo uriInfo;

    /**
     * Constructor.
     *
     * @param clusterConfigService The cluster configuration service to use.
     */
    @Inject
    public ClusterConfigResource(final ClusterConfigService clusterConfigService) {
        this.clusterConfigService = clusterConfigService;
    }

    /**
     * Create cluster configuration.
     *
     * @param cluster contains the cluster information to create
     * @return The created cluster
     * @throws GenieException For any error
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Create a cluster",
            notes = "Create a cluster from the supplied information.",
            response = Cluster.class
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_CREATED,
                    message = "Created",
                    response = Cluster.class
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_CONFLICT,
                    message = "A cluster with the supplied id already exists"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Response createCluster(
            @ApiParam(
                    value = "The cluster to create.",
                    required = true
            )
            final Cluster cluster
    ) throws GenieException {
        LOG.info("Called to create new cluster " + cluster);
        final Cluster createdCluster = this.clusterConfigService.createCluster(cluster);
        return Response.created(
                this.uriInfo.getAbsolutePathBuilder().path(createdCluster.getId()).build()).
                entity(createdCluster).
                build();
    }

    /**
     * Get cluster configuration from unique id.
     *
     * @param id id for the cluster
     * @return the cluster
     * @throws GenieException For any error
     */
    @GET
    @Path("/{id}")
    @ApiOperation(
            value = "Find a cluster by id",
            notes = "Get the cluster by id if it exists",
            response = Cluster.class
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Cluster getCluster(
            @ApiParam(
                    value = "Id of the cluster to get.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id: " + id);
        return this.clusterConfigService.getCluster(id);
    }

    /**
     * Get cluster config based on user params. If empty strings are passed for
     * they are treated as nulls (not false).
     *
     * @param name          cluster name (can be a pattern)
     * @param statuses      valid types - Types.ClusterStatus
     * @param tags          tags for the cluster
     * @param minUpdateTime min time when cluster configuration was updated
     * @param maxUpdateTime max time when cluster configuration was updated
     * @param limit         number of entries to return
     * @param page          page number
     * @param descending    Whether results returned in descending or ascending order
     * @param orderBys      The fields to order the results by
     * @return the Clusters found matching the criteria
     * @throws GenieException For any error
     */
    @GET
    @ApiOperation(
            value = "Find clusters",
            notes = "Find clusters by the submitted criteria.",
            response = Cluster.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "If one of status is invalid"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public List<Cluster> getClusters(
            @ApiParam(
                    value = "Name of the cluster."
            )
            @QueryParam("name")
            final String name,
            @ApiParam(
                    value = "Status of the cluster."
            )
            @QueryParam("status")
            final Set<String> statuses,
            @ApiParam(
                    value = "Tags for the cluster.",
                    allowableValues = "UP, OUT_OF_SERVICE, TERMINATED"
            )
            @QueryParam("tag")
            final Set<String> tags,
            @ApiParam(
                    value = "Minimum time threshold for cluster update"
            )
            @QueryParam("minUpdateTime")
            final Long minUpdateTime,
            @ApiParam(
                    value = "Maximum time threshold for cluster update"
            )
            @QueryParam("maxUpdateTime")
            final Long maxUpdateTime,
            @ApiParam(
                    value = "The page to start on."
            )
            @QueryParam("page")
            @DefaultValue("0")
            int page,
            @ApiParam(
                    value = "Max number of results per page."
            )
            @QueryParam("limit")
            @DefaultValue("1024")
            int limit,
            @ApiParam(
                    value = "Whether results should be sorted in descending or ascending order. Defaults to descending"
            )
            @QueryParam("descending")
            @DefaultValue("true")
            boolean descending,
            @ApiParam(
                    value = "The fields to order the results by. Must not be collection fields. Default is updated."
            )
            @QueryParam("orderBy")
            final Set<String> orderBys
    ) throws GenieException {
        LOG.info(
                "Called [name | statuses | tags | minUpdateTime | maxUpdateTime | page | limit | descending | orderBys]"
        );
        LOG.info(
                name
                        + " | "
                        + statuses
                        + " | "
                        + tags
                        + " | "
                        + minUpdateTime
                        + " | "
                        + maxUpdateTime
                        + " | "
                        + page
                        + " | "
                        + limit
                        + " | "
                        + descending
                        + " | "
                        + orderBys
        );
        //Create this conversion internal in case someone uses lower case by accident?
        Set<ClusterStatus> enumStatuses = null;
        if (!statuses.isEmpty()) {
            enumStatuses = EnumSet.noneOf(ClusterStatus.class);
            for (final String status : statuses) {
                if (StringUtils.isNotBlank(status)) {
                    enumStatuses.add(ClusterStatus.parse(status));
                }
            }
        }
        return this.clusterConfigService.getClusters(
                name, enumStatuses, tags, minUpdateTime, maxUpdateTime, page, limit, descending, orderBys
        );
    }

    /**
     * Update a cluster configuration.
     *
     * @param id            unique if for cluster to update
     * @param updateCluster contains the cluster information to update
     * @return the updated cluster
     * @throws GenieException For any error
     */
    @PUT
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update a cluster",
            notes = "Update a cluster from the supplied information.",
            response = Cluster.class
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster to update not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Cluster updateCluster(
            @ApiParam(
                    value = "Id of the cluster to update.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The cluster information to update with.",
                    required = true
            )
            final Cluster updateCluster
    ) throws GenieException {
        LOG.info("Called to update cluster with id " + id + " update fields " + updateCluster);
        return this.clusterConfigService.updateCluster(id, updateCluster);
    }

    /**
     * Delete a cluster configuration.
     *
     * @param id unique id for cluster to delete
     * @return the deleted cluster
     * @throws GenieException For any error
     */
    @DELETE
    @Path("/{id}")
    @ApiOperation(
            value = "Delete a cluster",
            notes = "Delete a cluster with the supplied id.",
            response = Cluster.class
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Cluster deleteCluster(
            @ApiParam(
                    value = "Id of the cluster to delete.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Delete called for id: " + id);
        return this.clusterConfigService.deleteCluster(id);
    }

    /**
     * Delete all clusters from database.
     *
     * @return All The deleted clusters
     * @throws GenieException For any error
     */
    @DELETE
    @ApiOperation(
            value = "Delete all clusters",
            notes = "Delete all available clusters and get them back.",
            response = Cluster.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public List<Cluster> deleteAllClusters() throws GenieException {
        LOG.info("called");
        return this.clusterConfigService.deleteAllClusters();
    }

    /**
     * Add new configuration files to a given cluster.
     *
     * @param id      The id of the cluster to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @return The active configurations for this cluster.
     * @throws GenieException For any error
     */
    @POST
    @Path("/{id}/configs")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new configuration files to a cluster",
            notes = "Add the supplied configuration files to the cluster with the supplied id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> addConfigsForCluster(
            @ApiParam(
                    value = "Id of the cluster to add configuration to.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The configuration files to add.",
                    required = true
            )
            final Set<String> configs
    ) throws GenieException {
        LOG.info("Called with id " + id + " and config " + configs);
        return this.clusterConfigService.addConfigsForCluster(id, configs);
    }

    /**
     * Get all the configuration files for a given cluster.
     *
     * @param id The id of the cluster to get the configuration files for. Not
     *           NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws GenieException For any error
     */
    @GET
    @Path("/{id}/configs")
    @ApiOperation(
            value = "Get the configuration files for a cluster",
            notes = "Get the configuration files for the cluster with the supplied id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> getConfigsForCluster(
            @ApiParam(
                    value = "Id of the cluster to get configurations for.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.clusterConfigService.getConfigsForCluster(id);
    }

    /**
     * Update the configuration files for a given cluster.
     *
     * @param id      The id of the cluster to update the configuration files for.
     *                Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     *                files with. Not null/empty/blank.
     * @return The new set of cluster configurations.
     * @throws GenieException For any error
     */
    @PUT
    @Path("/{id}/configs")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update configuration files for an cluster",
            notes = "Replace the existing configuration files for cluster with given id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> updateConfigsForCluster(
            @ApiParam(
                    value = "Id of the cluster to update configurations for.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The configuration files to replace existing with.",
                    required = true
            )
            final Set<String> configs
    ) throws GenieException {
        LOG.info("Called with id " + id + " and configs " + configs);
        return this.clusterConfigService.updateConfigsForCluster(id, configs);
    }

    /**
     * Add new commands to the given cluster.
     *
     * @param id       The id of the cluster to add the commands to. Not
     *                 null/empty/blank.
     * @param commands The commands to add. Not null.
     * @return The active commands for this cluster.
     * @throws GenieException For any error
     */
    @POST
    @Path("/{id}/commands")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new commands to a cluster",
            notes = "Add the supplied commands to the cluster with the supplied id."
                    + " commands should already have been created.",
            response = Command.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public List<Command> addCommandsForCluster(
            @ApiParam(
                    value = "Id of the cluster to add commands to.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The commands to add.",
                    required = true
            )
            final List<Command> commands
    ) throws GenieException {
        LOG.info("Called with id " + id + " and commands " + commands);
        return this.clusterConfigService.addCommandsForCluster(id, commands);
    }

    /**
     * Get all the commands configured for a given cluster.
     *
     * @param id The id of the cluster to get the command files for. Not
     *           NULL/empty/blank.
     * @return The active set of commands for the cluster.
     * @throws GenieException For any error
     */
    @GET
    @Path("/{id}/commands")
    @ApiOperation(
            value = "Get the commands for a cluster",
            notes = "Get the commands for the cluster with the supplied id.",
            response = Command.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public List<Command> getCommandsForCluster(
            @ApiParam(
                    value = "Id of the cluster to get commands for.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @QueryParam("status")
            final Set<String> statuses
    ) throws GenieException {
        LOG.info("Called with id " + id + " status " + statuses);

        Set<CommandStatus> enumStatuses = null;
        if (!statuses.isEmpty()) {
            enumStatuses = EnumSet.noneOf(CommandStatus.class);
            for (final String status : statuses) {
                if (StringUtils.isNotBlank(status)) {
                    enumStatuses.add(CommandStatus.parse(status));
                }
            }
        }

        return this.clusterConfigService.getCommandsForCluster(id, enumStatuses);
    }

    /**
     * Update the commands for a given cluster.
     *
     * @param id       The id of the cluster to update the configuration files for.
     *                 Not null/empty/blank.
     * @param commands The commands to replace existing applications with. Not
     *                 null/empty/blank.
     * @return The new set of commands for the cluster.
     * @throws GenieException For any error
     */
    @PUT
    @Path("/{id}/commands")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update the commands for an cluster",
            notes = "Replace the existing commands for cluster with given id.",
            response = Command.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public List<Command> updateCommandsForCluster(
            @ApiParam(
                    value = "Id of the cluster to update commands for.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The commands to replace existing with. Should already be created",
                    required = true
            )
            final List<Command> commands
    ) throws GenieException {
        LOG.info("Called with id " + id + " and commands " + commands);
        return this.clusterConfigService.updateCommandsForCluster(id, commands);
    }

    /**
     * Remove the all commands from a given cluster.
     *
     * @param id The id of the cluster to delete the commands from. Not
     *           null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException For any error
     */
    @DELETE
    @Path("/{id}/commands")
    @ApiOperation(
            value = "Remove all commands from an cluster",
            notes = "Remove all the commands from the cluster with given id.",
            response = Command.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public List<Command> removeAllCommandsForCluster(
            @ApiParam(
                    value = "Id of the cluster to delete from.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.clusterConfigService.removeAllCommandsForCluster(id);
    }

    /**
     * Remove an command from a given cluster.
     *
     * @param id    The id of the cluster to delete the command from. Not
     *              null/empty/blank.
     * @param cmdId The id of the command to remove. Not null/empty/blank.
     * @return The active set of commands for the cluster.
     * @throws GenieException For any error
     */
    @DELETE
    @Path("/{id}/commands/{cmdId}")
    @ApiOperation(
            value = "Remove a command from a cluster",
            notes = "Remove the given command from the cluster with given id.",
            response = Command.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public List<Command> removeCommandForCluster(
            @ApiParam(
                    value = "Id of the cluster to delete from.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The id of the command to remove.",
                    required = true
            )
            @PathParam("cmdId")
            final String cmdId
    ) throws GenieException {
        LOG.info("Called with id " + id + " and command id " + cmdId);
        return this.clusterConfigService.removeCommandForCluster(id, cmdId);
    }

    /**
     * Add new tags to a given cluster.
     *
     * @param id   The id of the cluster to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @return The active tags for this cluster.
     * @throws GenieException For any error
     */
    @POST
    @Path("/{id}/tags")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new tags to a cluster",
            notes = "Add the supplied tags to the cluster with the supplied id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> addTagsForCluster(
            @ApiParam(
                    value = "Id of the cluster to add configuration to.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The tags to add.",
                    required = true
            )
            final Set<String> tags
    ) throws GenieException {
        LOG.info("Called with id " + id + " and tags " + tags);
        return this.clusterConfigService.addTagsForCluster(id, tags);
    }

    /**
     * Get all the tags for a given cluster.
     *
     * @param id The id of the cluster to get the tags for. Not
     *           NULL/empty/blank.
     * @return The active set of tags.
     * @throws GenieException For any error
     */
    @GET
    @Path("/{id}/tags")
    @ApiOperation(
            value = "Get the tags for a cluster",
            notes = "Get the tags for the cluster with the supplied id.",
            response = String.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> getTagsForCluster(
            @ApiParam(
                    value = "Id of the cluster to get tags for.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.clusterConfigService.getTagsForCluster(id);
    }

    /**
     * Update the tags for a given cluster.
     *
     * @param id   The id of the cluster to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing configuration
     *             files with. Not null/empty/blank.
     * @return The new set of cluster tags.
     * @throws GenieException For any error
     */
    @PUT
    @Path("/{id}/tags")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update tags for a cluster",
            notes = "Replace the existing tags for cluster with given id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> updateTagsForCluster(
            @ApiParam(
                    value = "Id of the cluster to update tags for.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The tags to replace existing with.",
                    required = true
            )
            final Set<String> tags
    ) throws GenieException {
        LOG.info("Called with id " + id + " and tags " + tags);
        return this.clusterConfigService.updateTagsForCluster(id, tags);
    }

    /**
     * Delete the all tags from a given cluster.
     *
     * @param id The id of the cluster to delete the tags from.
     *           Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException For any error
     */
    @DELETE
    @Path("/{id}/tags")
    @ApiOperation(
            value = "Remove all tags from a cluster",
            notes = "Remove all the tags from the cluster with given id.  Note that the genie name space tags"
                    + "prefixed with genie.id and genie.name cannot be deleted.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> removeAllTagsForCluster(
            @ApiParam(
                    value = "Id of the cluster to delete from.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.clusterConfigService.removeAllTagsForCluster(id);
    }

    /**
     * Remove an tag from a given cluster.
     *
     * @param id  The id of the cluster to delete the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @return The active set of tags for the cluster.
     * @throws GenieException For any error
     */
    @DELETE
    @Path("/{id}/tags/{tag}")
    @ApiOperation(
            value = "Remove a tag from a cluster",
            notes = "Remove the given tag from the cluster with given id. Note that the genie name space tags"
                    + "prefixed with genie.id and genie.name cannot be deleted.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Cluster not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> removeTagForCluster(
            @ApiParam(
                    value = "Id of the cluster to delete from.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The tag to remove.",
                    required = true
            )
            @PathParam("tag")
            final String tag
    ) throws GenieException {
        LOG.info("Called with id " + id + " and tag " + tag);
        return this.clusterConfigService.removeTagForCluster(id, tag);
    }
}
