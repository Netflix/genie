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

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.web.hateoas.assemblers.ClusterResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.CommandResourceAssembler;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import com.netflix.genie.web.hateoas.resources.CommandResource;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ResponseHeader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.PagedResources;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.HttpURLConnection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * REST end-point for supporting clusters.
 *
 * @author tgianos
 * @since 3.0.0
 */
@RestController
@RequestMapping(value = "/api/v3/clusters")
@Api(value = "clusters", tags = "clusters", description = "Manage the available clusters")
public class ClusterRestController {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterRestController.class);

    private final ClusterService clusterService;
    private final ClusterResourceAssembler clusterResourceAssembler;
    private final CommandResourceAssembler commandResourceAssembler;

    /**
     * Constructor.
     *
     * @param clusterService           The cluster configuration service to use.
     * @param clusterResourceAssembler The assembler to use to convert clusters to cluster HAL resources
     * @param commandResourceAssembler The assembler to use to convert commands to command HAL resources
     */
    @Autowired
    public ClusterRestController(
            final ClusterService clusterService,
            final ClusterResourceAssembler clusterResourceAssembler,
            final CommandResourceAssembler commandResourceAssembler
    ) {
        this.clusterService = clusterService;
        this.clusterResourceAssembler = clusterResourceAssembler;
        this.commandResourceAssembler = commandResourceAssembler;
    }

    /**
     * Create cluster configuration.
     *
     * @param cluster contains the cluster information to create
     * @return The created cluster
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(
            value = "Create a cluster",
            notes = "Create a cluster from the supplied information."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_CREATED,
                    message = "Cluster created successfully.",
                    responseHeaders = {@ResponseHeader(name = HttpHeaders.LOCATION, response = String.class)}
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
    public ResponseEntity<Void> createCluster(
            @ApiParam(
                    value = "The cluster to create.",
                    required = true
            )
            @RequestBody
            final Cluster cluster
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to create new cluster " + cluster);
        }
        final String id = this.clusterService.createCluster(cluster);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setLocation(
                ServletUriComponentsBuilder
                        .fromCurrentRequest()
                        .path("/{id}")
                        .buildAndExpand(id)
                        .toUri()
        );
        return new ResponseEntity<>(httpHeaders, HttpStatus.CREATED);
    }

    /**
     * Get cluster configuration from unique id.
     *
     * @param id id for the cluster
     * @return the cluster
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
            value = "Find a cluster by id",
            notes = "Get the cluster by id if it exists",
            response = ClusterResource.class
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
    public ClusterResource getCluster(
            @ApiParam(
                    value = "Id of the cluster to get.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id: " + id);
        }
        return this.clusterResourceAssembler.toResource(this.clusterService.getCluster(id));
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
     * @param page          The page to get
     * @param assembler     The paged resources assembler to use
     * @return the Clusters found matching the criteria
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
            value = "Find clusters",
            notes = "Find clusters by the submitted criteria.",
            response = ClusterResource.class,
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
    public PagedResources<ClusterResource> getClusters(
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
            @PageableDefault(page = 0, size = 64, sort = {"updated"}, direction = Sort.Direction.DESC)
            final Pageable page,
            final PagedResourcesAssembler<Cluster> assembler
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Called "
                            + "[name "
                            + "| statuses "
                            + "| tags "
                            + "| minUpdateTime "
                            + "| maxUpdateTime "
                            + "| page]"
            );
            LOG.debug(
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
            );
        }
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

        return assembler.toResource(
                this.clusterService.getClusters(name, enumStatuses, tags, minUpdateTime, maxUpdateTime, page),
                this.clusterResourceAssembler
        );
    }

    /**
     * Update a cluster configuration.
     *
     * @param id            unique if for cluster to update
     * @param updateCluster contains the cluster information to update
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Update a cluster",
            notes = "Update a cluster from the supplied information."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successful update"
            ),
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
    public void updateCluster(
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to update cluster with id " + id + " update fields " + updateCluster);
        }
        this.clusterService.updateCluster(id, updateCluster);
    }

    /**
     * Delete a cluster configuration.
     *
     * @param id unique id for cluster to delete
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Delete a cluster",
            notes = "Delete a cluster with the supplied id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successful delete"
            ),
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
    public void deleteCluster(
            @ApiParam(
                    value = "Id of the cluster to delete.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Delete called for id: " + id);
        }
        this.clusterService.deleteCluster(id);
    }

    /**
     * Delete all clusters from database.
     *
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Delete all clusters",
            notes = "Delete all available clusters and get them back."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successful delete"
            ),
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
    public void deleteAllClusters() throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("called");
        }
        this.clusterService.deleteAllClusters();
    }

    /**
     * Add new configuration files to a given cluster.
     *
     * @param id      The id of the cluster to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Add new configuration files to a cluster",
            notes = "Add the supplied configuration files to the cluster with the supplied id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successful add"
            ),
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
    public void addConfigsForCluster(
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and config " + configs);
        }
        this.clusterService.addConfigsForCluster(id, configs);
    }

    /**
     * Get all the configuration files for a given cluster.
     *
     * @param id The id of the cluster to get the configuration files for. Not
     *           NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        return this.clusterService.getConfigsForCluster(id);
    }

    /**
     * Update the configuration files for a given cluster.
     *
     * @param id      The id of the cluster to update the configuration files for.
     *                Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     *                files with. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Update configuration files for an cluster",
            notes = "Replace the existing configuration files for cluster with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successful update"
            ),
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
    public void updateConfigsForCluster(
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and configs " + configs);
        }
        this.clusterService.updateConfigsForCluster(id, configs);
    }

    /**
     * Delete the all configuration files from a given cluster.
     *
     * @param id The id of the cluster to delete the configuration files from.
     *           Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Remove all configuration files from a cluster",
            notes = "Remove all the configuration files from the cluster with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successful delete"
            ),
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
    public void removeAllConfigsForCluster(
            @ApiParam(
                    value = "Id of the cluster to delete from.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        this.clusterService.removeAllConfigsForCluster(id);
    }

    /**
     * Add new tags to a given cluster.
     *
     * @param id   The id of the cluster to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Add new tags to a cluster",
            notes = "Add the supplied tags to the cluster with the supplied id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successful add"
            ),
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
    public void addTagsForCluster(
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and tags " + tags);
        }
        this.clusterService.addTagsForCluster(id, tags);
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
    @ResponseStatus(HttpStatus.OK)
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        return this.clusterService.getTagsForCluster(id);
    }

    /**
     * Update the tags for a given cluster.
     *
     * @param id   The id of the cluster to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing configuration
     *             files with. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Update tags for a cluster",
            notes = "Replace the existing tags for cluster with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully updated"
            ),
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
    public void updateTagsForCluster(
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and tags " + tags);
        }
        this.clusterService.updateTagsForCluster(id, tags);
    }

    /**
     * Delete the all tags from a given cluster.
     *
     * @param id The id of the cluster to delete the tags from.
     *           Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Remove all tags from a cluster",
            notes = "Remove all the tags from the cluster with given id.  Note that the genie name space tags"
                    + "prefixed with genie.id and genie.name cannot be deleted."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
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
    public void removeAllTagsForCluster(
            @ApiParam(
                    value = "Id of the cluster to delete from.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        this.clusterService.removeAllTagsForCluster(id);
    }

    /**
     * Remove an tag from a given cluster.
     *
     * @param id  The id of the cluster to delete the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/tags/{tag}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Remove a tag from a cluster",
            notes = "Remove the given tag from the cluster with given id. Note that the genie name space tags"
                    + "prefixed with genie.id and genie.name cannot be deleted."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
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
    public void removeTagForCluster(
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and tag " + tag);
        }
        this.clusterService.removeTagForCluster(id, tag);
    }

    /**
     * Add new commandIds to the given cluster.
     *
     * @param id         The id of the cluster to add the commandIds to. Not
     *                   null/empty/blank.
     * @param commandIds The ids of the commandIds to add. Not null.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/commands", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Add new commandIds to a cluster",
            notes = "Add the supplied commandIds to the cluster with the supplied id."
                    + " commandIds should already have been created."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully added"
            ),
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
    public void addCommandsForCluster(
            @ApiParam(
                    value = "Id of the cluster to add commandIds to.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The ids of the commandIds to add.",
                    required = true
            )
            @RequestBody
            final List<String> commandIds
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and commandIds " + commandIds);
        }
        this.clusterService.addCommandsForCluster(id, commandIds);
    }

    /**
     * Get all the commandIds configured for a given cluster.
     *
     * @param id       The id of the cluster to get the command files for. Not
     *                 NULL/empty/blank.
     * @param statuses The various statuses to return commandIds for.
     * @return The active set of commandIds for the cluster.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/commands", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
            value = "Get the commandIds for a cluster",
            notes = "Get the commandIds for the cluster with the supplied id.",
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
    public List<CommandResource> getCommandsForCluster(
            @ApiParam(
                    value = "Id of the cluster to get commandIds for.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The statuses of the commandIds to find.",
                    allowableValues = "ACTIVE, DEPRECATED, INACTIVE"
            )
            @RequestParam(value = "status", required = false)
            final Set<String> statuses
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " status " + statuses);
        }

        Set<CommandStatus> enumStatuses = null;
        if (statuses != null && !statuses.isEmpty()) {
            enumStatuses = EnumSet.noneOf(CommandStatus.class);
            for (final String status : statuses) {
                if (StringUtils.isNotBlank(status)) {
                    enumStatuses.add(CommandStatus.parse(status));
                }
            }
        }

        return this.clusterService.getCommandsForCluster(id, enumStatuses)
                .stream()
                .map(this.commandResourceAssembler::toResource)
                .collect(Collectors.toList());
    }

    /**
     * Set the commandIds for a given cluster.
     *
     * @param id         The id of the cluster to update the configuration files for.
     *                   Not null/empty/blank.
     * @param commandIds The ids of the commands to replace existing commands with. Not
     *                   null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/commands", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Update the commands for a cluster",
            notes = "Replace the existing commands for cluster with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully updated"
            ),
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
    public void setCommandsForCluster(
            @ApiParam(
                    value = "Id of the cluster to update commandIds for.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The ids of the commands to replace existing commands with. Should already be created",
                    required = true
            )
            @RequestBody
            final List<String> commandIds
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and commandIds " + commandIds);
        }
        this.clusterService.updateCommandsForCluster(id, commandIds);
    }

    /**
     * Remove the all commandIds from a given cluster.
     *
     * @param id The id of the cluster to delete the commandIds from. Not
     *           null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/commands", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Remove all commandIds from an cluster",
            notes = "Remove all the commandIds from the cluster with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
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
    public void removeAllCommandsForCluster(
            @ApiParam(
                    value = "Id of the cluster to delete from.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        this.clusterService.removeAllCommandsForCluster(id);
    }

    /**
     * Remove an command from a given cluster.
     *
     * @param id        The id of the cluster to delete the command from. Not
     *                  null/empty/blank.
     * @param commandId The id of the command to remove. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/commands/{commandId}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Remove a command from a cluster",
            notes = "Remove the given command from the cluster with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
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
    public void removeCommandForCluster(
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and command id " + commandId);
        }
        this.clusterService.removeCommandForCluster(id, commandId);
    }
}
