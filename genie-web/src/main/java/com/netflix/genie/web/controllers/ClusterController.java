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
package com.netflix.genie.web.controllers;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterStatus;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.server.services.ClusterConfigService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

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
@RestController
@RequestMapping(value = "/api/v3/clusters", produces = MediaType.APPLICATION_JSON_VALUE)
@Api(value = "/api/v3/clusters", tags = "clusters", description = "Manage the available clusters")
public final class ClusterController {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterController.class);

    private final ClusterConfigService clusterConfigService;

    /**
     * Constructor.
     *
     * @param clusterConfigService The cluster configuration service to use.
     */
    @Autowired
    public ClusterController(final ClusterConfigService clusterConfigService) {
        this.clusterConfigService = clusterConfigService;
    }

    /**
     * Create cluster configuration.
     *
     * @param cluster contains the cluster information to create
     * @return The created cluster
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
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
    public ResponseEntity<Cluster> createCluster(
            @ApiParam(
                    value = "The cluster to create.",
                    required = true
            )
            @RequestBody
            final Cluster cluster
    ) throws GenieException {
        LOG.info("Called to create new cluster " + cluster);
        final Cluster createdCluster = this.clusterConfigService.createCluster(cluster);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setLocation(
                ServletUriComponentsBuilder
                        .fromCurrentRequest()
                        .path("/{id}")
                        .buildAndExpand(createdCluster.getId())
                        .toUri()
        );
        return new ResponseEntity<>(createdCluster, httpHeaders, HttpStatus.CREATED);
    }

    /**
     * Get cluster configuration from unique id.
     *
     * @param id id for the cluster
     * @return the cluster
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
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
            @PathVariable("id")
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
    @RequestMapping(method = RequestMethod.GET)
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
            @RequestParam(value = "name", required = false)
            final String name,
            @ApiParam(
                    value = "Status of the cluster.",
                    allowableValues = "UP, OUT_OF_SERVICE, TERMINATED"
            )
            @RequestParam(value = "status", required = false)
            final Set<String> statuses,
            @ApiParam(
                    value = "Tags for the cluster."
            )
            @RequestParam(value = "tag", required = false)
            final Set<String> tags,
            @ApiParam(
                    value = "Minimum time threshold for cluster update"
            )
            @RequestParam(value = "minUpdateTime", required = false)
            final Long minUpdateTime,
            @ApiParam(
                    value = "Maximum time threshold for cluster update"
            )
            @RequestParam(value = "maxUpdateTime", required = false)
            final Long maxUpdateTime,
            @ApiParam(
                    value = "The page to start on."
            )
            @RequestParam(value = "page", defaultValue = "0")
            final int page,
            @ApiParam(
                    value = "Max number of results per page."
            )
            @RequestParam(value = "limit", defaultValue = "1024")
            final int limit,
            @ApiParam(
                    value = "Whether results should be sorted in descending or ascending order. Defaults to descending"
            )
            @RequestParam(value = "descending", defaultValue = "true")
            final boolean descending,
            @ApiParam(
                    value = "The fields to order the results by. Must not be collection fields. Default is updated."
            )
            @RequestParam(value = "orderBy", required = false)
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
        if (statuses != null && !statuses.isEmpty()) {
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
    @RequestMapping(value = "/{id}", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
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
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The cluster information to update with.",
                    required = true
            )
            @RequestBody
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
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
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
            @PathVariable("id")
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
    @RequestMapping(method = RequestMethod.DELETE)
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
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation(
            value = "Add new configuration files to a cluster",
            notes = "Add the supplied configuration files to the cluster with the supplied id.",
            response = String.class,
            responseContainer = "Set"
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
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The configuration files to add.",
                    required = true
            )
            @RequestBody
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
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.GET)
    @ApiOperation(
            value = "Get the configuration files for a cluster",
            notes = "Get the configuration files for the cluster with the supplied id.",
            response = String.class,
            responseContainer = "Set"
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
            @PathVariable("id")
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
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation(
            value = "Update configuration files for an cluster",
            notes = "Replace the existing configuration files for cluster with given id.",
            response = String.class,
            responseContainer = "Set"
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
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The configuration files to replace existing with.",
                    required = true
            )
            @RequestBody
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
    @RequestMapping(value = "/{id}/commands", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
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
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The commands to add.",
                    required = true
            )
            @RequestBody
            final List<Command> commands
    ) throws GenieException {
        LOG.info("Called with id " + id + " and commands " + commands);
        return this.clusterConfigService.addCommandsForCluster(id, commands);
    }

    /**
     * Get all the commands configured for a given cluster.
     *
     * @param id       The id of the cluster to get the command files for. Not
     *                 NULL/empty/blank.
     * @param statuses The various statuses to return commands for.
     * @return The active set of commands for the cluster.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/commands", method = RequestMethod.GET)
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
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The statuses of the commands to find.",
                    allowableValues = "ACTIVE, DEPRECATED, INACTIVE"
            )
            @RequestParam(value = "status", required = false)
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
    @RequestMapping(value = "/{id}/commands", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
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
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The commands to replace existing with. Should already be created",
                    required = true
            )
            @RequestBody
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
    @RequestMapping(value = "/{id}/commands", method = RequestMethod.DELETE)
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
            @PathVariable("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.clusterConfigService.removeAllCommandsForCluster(id);
    }

    /**
     * Remove an command from a given cluster.
     *
     * @param id        The id of the cluster to delete the command from. Not
     *                  null/empty/blank.
     * @param commandId The id of the command to remove. Not null/empty/blank.
     * @return The active set of commands for the cluster.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/commands/{commandId}", method = RequestMethod.DELETE)
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
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The id of the command to remove.",
                    required = true
            )
            @PathVariable("commandId")
            final String commandId
    ) throws GenieException {
        LOG.info("Called with id " + id + " and command id " + commandId);
        return this.clusterConfigService.removeCommandForCluster(id, commandId);
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
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation(
            value = "Add new tags to a cluster",
            notes = "Add the supplied tags to the cluster with the supplied id.",
            response = String.class,
            responseContainer = "Set"
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
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The tags to add.",
                    required = true
            )
            @RequestBody
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
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.GET)
    @ApiOperation(
            value = "Get the tags for a cluster",
            notes = "Get the tags for the cluster with the supplied id.",
            response = String.class,
            responseContainer = "Set")
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
            @PathVariable("id")
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
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation(
            value = "Update tags for a cluster",
            notes = "Replace the existing tags for cluster with given id.",
            response = String.class,
            responseContainer = "Set"
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
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The tags to replace existing with.",
                    required = true
            )
            @RequestBody
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
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.DELETE)
    @ApiOperation(
            value = "Remove all tags from a cluster",
            notes = "Remove all the tags from the cluster with given id.  Note that the genie name space tags"
                    + "prefixed with genie.id and genie.name cannot be deleted.",
            response = String.class,
            responseContainer = "Set"
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
            @PathVariable("id")
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
    @RequestMapping(value = "/{id}/tags/{tag}", method = RequestMethod.DELETE)
    @ApiOperation(
            value = "Remove a tag from a cluster",
            notes = "Remove the given tag from the cluster with given id. Note that the genie name space tags"
                    + "prefixed with genie.id and genie.name cannot be deleted.",
            response = String.class,
            responseContainer = "Set"
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
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The tag to remove.",
                    required = true
            )
            @PathVariable("tag")
            final String tag
    ) throws GenieException {
        LOG.info("Called with id " + id + " and tag " + tag);
        return this.clusterConfigService.removeTagForCluster(id, tag);
    }
}
