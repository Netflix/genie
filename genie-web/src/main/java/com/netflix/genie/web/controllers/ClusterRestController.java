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

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.hateoas.assemblers.ClusterResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.CommandResourceAssembler;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import com.netflix.genie.web.hateoas.resources.CommandResource;
import com.netflix.genie.web.services.ClusterPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.PagedResources;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.validation.Valid;
import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
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
@Slf4j
public class
ClusterRestController {

    private final ClusterPersistenceService clusterPersistenceService;
    private final ClusterResourceAssembler clusterResourceAssembler;
    private final CommandResourceAssembler commandResourceAssembler;

    /**
     * Constructor.
     *
     * @param clusterPersistenceService The cluster configuration service to use.
     * @param clusterResourceAssembler  The assembler to use to convert clusters to cluster HAL resources
     * @param commandResourceAssembler  The assembler to use to convert commands to command HAL resources
     */
    @Autowired
    public ClusterRestController(
        final ClusterPersistenceService clusterPersistenceService,
        final ClusterResourceAssembler clusterResourceAssembler,
        final CommandResourceAssembler commandResourceAssembler
    ) {
        this.clusterPersistenceService = clusterPersistenceService;
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
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<Void> createCluster(@RequestBody @Valid final Cluster cluster) throws GenieException {
        log.debug("Called to create new cluster {}", cluster);
        final String id = this.clusterPersistenceService.createCluster(DtoConverters.toV4ClusterRequest(cluster));
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
    @GetMapping(value = "/{id}", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ClusterResource getCluster(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id: {}", id);
        return this.clusterResourceAssembler.toResource(
            DtoConverters.toV3Cluster(this.clusterPersistenceService.getCluster(id))
        );
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
    @GetMapping(produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public PagedResources<ClusterResource> getClusters(
        @RequestParam(value = "name", required = false) final String name,
        @RequestParam(value = "status", required = false) final Set<String> statuses,
        @RequestParam(value = "tag", required = false) final Set<String> tags,
        @RequestParam(value = "minUpdateTime", required = false) final Long minUpdateTime,
        @RequestParam(value = "maxUpdateTime", required = false) final Long maxUpdateTime,
        @PageableDefault(size = 64, sort = {"updated"}, direction = Sort.Direction.DESC) final Pageable page,
        final PagedResourcesAssembler<Cluster> assembler
    ) throws GenieException {
        log.debug("Called [name | statuses | tags | minUpdateTime | maxUpdateTime | page]");
        log.debug("{} | {} | {} | {} | {} | {}", name, statuses, tags, minUpdateTime, maxUpdateTime, page);
        //Create this conversion internal in case someone uses lower case by accident?
        Set<ClusterStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(ClusterStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(ClusterStatus.parse(status));
            }
        }

        final Page<Cluster> clusters;
        if (tags != null && tags.stream().filter(tag -> tag.startsWith(DtoConverters.GENIE_ID_PREFIX)).count() >= 1L) {
            // TODO: This doesn't take into account others as compounded find...not sure if good or bad
            final List<Cluster> clusterList = Lists.newArrayList();
            final int prefixLength = DtoConverters.GENIE_ID_PREFIX.length();
            tags
                .stream()
                .filter(tag -> tag.startsWith(DtoConverters.GENIE_ID_PREFIX))
                .forEach(
                    tag -> {
                        final String id = tag.substring(prefixLength);
                        try {
                            clusterList.add(DtoConverters.toV3Cluster(this.clusterPersistenceService.getCluster(id)));
                        } catch (final GenieException ge) {
                            log.debug("No cluster with id {} found", id, ge);
                        }
                    }
                );
            clusters = new PageImpl<>(clusterList);
        } else if (tags != null
            && tags.stream().filter(tag -> tag.startsWith(DtoConverters.GENIE_NAME_PREFIX)).count() >= 1L) {
            final Set<String> finalTags = tags
                .stream()
                .filter(tag -> !tag.startsWith(DtoConverters.GENIE_NAME_PREFIX))
                .collect(Collectors.toSet());
            if (name == null) {
                final Optional<String> finalName = tags
                    .stream()
                    .filter(tag -> tag.startsWith(DtoConverters.GENIE_NAME_PREFIX))
                    .map(tag -> tag.substring(DtoConverters.GENIE_NAME_PREFIX.length()))
                    .findFirst();

                clusters = this.clusterPersistenceService
                    .getClusters(
                        finalName.orElse(null),
                        enumStatuses,
                        finalTags,
                        minUpdateTime == null ? null : Instant.ofEpochMilli(minUpdateTime),
                        maxUpdateTime == null ? null : Instant.ofEpochMilli(maxUpdateTime),
                        page
                    )
                    .map(DtoConverters::toV3Cluster);
            } else {
                clusters = this.clusterPersistenceService
                    .getClusters(
                        name,
                        enumStatuses,
                        finalTags,
                        minUpdateTime == null ? null : Instant.ofEpochMilli(minUpdateTime),
                        maxUpdateTime == null ? null : Instant.ofEpochMilli(maxUpdateTime),
                        page
                    )
                    .map(DtoConverters::toV3Cluster);
            }
        } else {
            clusters = this.clusterPersistenceService
                .getClusters(
                    name,
                    enumStatuses,
                    tags,
                    minUpdateTime == null ? null : Instant.ofEpochMilli(minUpdateTime),
                    maxUpdateTime == null ? null : Instant.ofEpochMilli(maxUpdateTime),
                    page
                )
                .map(DtoConverters::toV3Cluster);
        }

        // Build the self link which will be used for the next, previous, etc links
        final Link self = ControllerLinkBuilder
            .linkTo(
                ControllerLinkBuilder
                    .methodOn(ClusterRestController.class)
                    .getClusters(
                        name,
                        statuses,
                        tags,
                        minUpdateTime,
                        maxUpdateTime,
                        page,
                        assembler
                    )
            ).withSelfRel();

        return assembler.toResource(
            clusters,
            this.clusterResourceAssembler,
            self
        );
    }

    /**
     * Update a cluster configuration.
     *
     * @param id            unique if for cluster to update
     * @param updateCluster contains the cluster information to update
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateCluster(
        @PathVariable("id") final String id,
        @RequestBody final Cluster updateCluster
    ) throws GenieException {
        log.debug("Called to update cluster with id {} update fields {}", id, updateCluster);
        this.clusterPersistenceService.updateCluster(id, DtoConverters.toV4Cluster(updateCluster));
    }

    /**
     * Patch a cluster using JSON Patch.
     *
     * @param id    The id of the cluster to patch
     * @param patch The JSON Patch instructions
     * @throws GenieException On error
     */
    @PatchMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void patchCluster(
        @PathVariable("id") final String id,
        @RequestBody final JsonPatch patch
    ) throws GenieException {
        log.debug("Called to patch cluster {} with patch {}", id, patch);

        final Cluster currentCluster = DtoConverters.toV3Cluster(this.clusterPersistenceService.getCluster(id));

        try {
            log.debug("Will patch cluster {}. Original state: {}", id, currentCluster);
            final JsonNode clusterNode = GenieObjectMapper.getMapper().valueToTree(currentCluster);
            final JsonNode postPatchNode = patch.apply(clusterNode);
            final Cluster patchedCluster = GenieObjectMapper.getMapper().treeToValue(postPatchNode, Cluster.class);
            log.debug("Finished patching cluster {}. New state: {}", id, patchedCluster);
            this.clusterPersistenceService.updateCluster(id, DtoConverters.toV4Cluster(patchedCluster));
        } catch (final JsonPatchException | IOException e) {
            log.error("Unable to patch cluster {} with patch {} due to exception.", id, patch, e);
            throw new GenieServerException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Delete a cluster configuration.
     *
     * @param id unique id for cluster to delete
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteCluster(@PathVariable("id") final String id) throws GenieException {
        log.debug("Delete called for id: {}", id);
        this.clusterPersistenceService.deleteCluster(id);
    }

    /**
     * Delete all clusters from database.
     *
     * @throws GenieException For any error
     */
    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteAllClusters() throws GenieException {
        log.debug("called");
        this.clusterPersistenceService.deleteAllClusters();
    }

    /**
     * Add new configuration files to a given cluster.
     *
     * @param id      The id of the cluster to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addConfigsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws GenieException {
        log.debug("Called with id {} and config {}", id, configs);
        this.clusterPersistenceService.addConfigsForCluster(id, configs);
    }

    /**
     * Get all the configuration files for a given cluster.
     *
     * @param id The id of the cluster to get the configuration files for. Not
     *           NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/configs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getConfigsForCluster(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return this.clusterPersistenceService.getConfigsForCluster(id);
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
    @PutMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateConfigsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws GenieException {
        log.debug("Called with id {} and configs {}", id, configs);
        this.clusterPersistenceService.updateConfigsForCluster(id, configs);
    }

    /**
     * Delete the all configuration files from a given cluster.
     *
     * @param id The id of the cluster to delete the configuration files from.
     *           Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/configs")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllConfigsForCluster(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        this.clusterPersistenceService.removeAllConfigsForCluster(id);
    }

    /**
     * Add new dependency files for a given cluster.
     *
     * @param id           The id of the cluster to add the dependency file to. Not
     *                     null/empty/blank.
     * @param dependencies The dependency files to add. Not null.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addDependenciesForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws GenieException {
        log.debug("Called with id {} and dependencies {}", id, dependencies);
        this.clusterPersistenceService.addDependenciesForCluster(id, dependencies);
    }

    /**
     * Get all the dependency files for a given cluster.
     *
     * @param id The id of the cluster to get the dependency files for. Not
     *           NULL/empty/blank.
     * @return The set of dependency files.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/dependencies", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getDependenciesForCluster(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return this.clusterPersistenceService.getDependenciesForCluster(id);
    }

    /**
     * Update the dependency files for a given cluster.
     *
     * @param id           The id of the cluster to update the dependency files for. Not
     *                     null/empty/blank.
     * @param dependencies The dependency files to replace existing dependency files with. Not
     *                     null/empty/blank.
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateDependenciesForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws GenieException {
        log.debug("Called with id {} and dependencies {}", id, dependencies);
        this.clusterPersistenceService.updateDependenciesForCluster(id, dependencies);
    }

    /**
     * Delete the all dependency files from a given cluster.
     *
     * @param id The id of the cluster to delete the dependency files from. Not
     *           null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/dependencies")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllDependenciesForCluster(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        this.clusterPersistenceService.removeAllDependenciesForCluster(id);
    }

    /**
     * Add new tags to a given cluster.
     *
     * @param id   The id of the cluster to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addTagsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws GenieException {
        log.debug("Called with id {} and tags {}", id, tags);
        this.clusterPersistenceService.addTagsForCluster(id, tags);
    }

    /**
     * Get all the tags for a given cluster.
     *
     * @param id The id of the cluster to get the tags for. Not
     *           NULL/empty/blank.
     * @return The active set of tags.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/tags", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getTagsForCluster(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return DtoConverters.toV3Cluster(this.clusterPersistenceService.getCluster(id)).getTags();
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
    @PutMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateTagsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws GenieException {
        log.debug("Called with id {} and tags {}", id, tags);
        this.clusterPersistenceService.updateTagsForCluster(id, tags);
    }

    /**
     * Delete the all tags from a given cluster.
     *
     * @param id The id of the cluster to delete the tags from.
     *           Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/tags")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllTagsForCluster(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        this.clusterPersistenceService.removeAllTagsForCluster(id);
    }

    /**
     * Remove an tag from a given cluster.
     *
     * @param id  The id of the cluster to delete the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/tags/{tag}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeTagForCluster(
        @PathVariable("id") final String id,
        @PathVariable("tag") final String tag
    ) throws GenieException {
        log.debug("Called with id {} and tag {}", id, tag);
        this.clusterPersistenceService.removeTagForCluster(id, tag);
    }

    /**
     * Add new commandIds to the given cluster.
     *
     * @param id         The id of the cluster to add the commandIds to. Not
     *                   null/empty/blank.
     * @param commandIds The ids of the commandIds to add. Not null.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/commands", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addCommandsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final List<String> commandIds
    ) throws GenieException {
        log.debug("Called with id {} and commandIds {}", id, commandIds);
        this.clusterPersistenceService.addCommandsForCluster(id, commandIds);
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
    @GetMapping(value = "/{id}/commands", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public List<CommandResource> getCommandsForCluster(
        @PathVariable("id") final String id,
        @RequestParam(value = "status", required = false) final Set<String> statuses
    ) throws GenieException {
        log.debug("Called with id {} status {}", id, statuses);

        Set<CommandStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(CommandStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(CommandStatus.parse(status));
            }
        }

        return this.clusterPersistenceService.getCommandsForCluster(id, enumStatuses)
            .stream()
            .map(DtoConverters::toV3Command)
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
    @PutMapping(value = "/{id}/commands", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void setCommandsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final List<String> commandIds
    ) throws GenieException {
        log.debug("Called with id {} and commandIds {}", id, commandIds);
        this.clusterPersistenceService.setCommandsForCluster(id, commandIds);
    }

    /**
     * Remove the all commandIds from a given cluster.
     *
     * @param id The id of the cluster to delete the commandIds from. Not
     *           null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/commands")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllCommandsForCluster(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        this.clusterPersistenceService.removeAllCommandsForCluster(id);
    }

    /**
     * Remove an command from a given cluster.
     *
     * @param id        The id of the cluster to delete the command from. Not
     *                  null/empty/blank.
     * @param commandId The id of the command to remove. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/commands/{commandId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeCommandForCluster(
        @PathVariable("id") final String id,
        @PathVariable("commandId") final String commandId
    ) throws GenieException {
        log.debug("Called with id {} and command id {}", id, commandId);
        this.clusterPersistenceService.removeCommandForCluster(id, commandId);
    }
}
