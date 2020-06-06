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
package com.netflix.genie.web.apis.rest.v3.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.external.dtos.v4.ClusterStatus;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.dtos.v4.converters.DtoConverters;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ClusterModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.EntityModelAssemblers;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.exceptions.checked.PreconditionFailedException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.PagedModel;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;
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

import javax.annotation.Nullable;
import javax.validation.Valid;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
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
public class ClusterRestController {

    private static final List<EntityModel<Command>> EMPTY_COMMAND_LIST = new ArrayList<>(0);

    private final PersistenceService persistenceService;
    private final ClusterModelAssembler clusterModelAssembler;

    /**
     * Constructor.
     *
     * @param dataServices          The {@link DataServices} encapsulation instance to use.
     * @param entityModelAssemblers The encapsulation of all available V3 resource assemblers
     */
    @Autowired
    public ClusterRestController(final DataServices dataServices, final EntityModelAssemblers entityModelAssemblers) {
        this.persistenceService = dataServices.getPersistenceService();
        this.clusterModelAssembler = entityModelAssemblers.getClusterModelAssembler();
    }

    /**
     * Create cluster configuration.
     *
     * @param cluster contains the cluster information to create
     * @return The created cluster
     * @throws IdAlreadyExistsException If there is a conflict for the id
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<Void> createCluster(
        @RequestBody @Valid final Cluster cluster
    ) throws IdAlreadyExistsException {
        log.info("[createCluster] Called to create new cluster {}", cluster);
        final String id = this.persistenceService.saveCluster(DtoConverters.toV4ClusterRequest(cluster));
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
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @GetMapping(value = "/{id}", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public EntityModel<Cluster> getCluster(@PathVariable("id") final String id) throws NotFoundException {
        log.info("[getCluster] Called with id: {}", id);
        return this.clusterModelAssembler.toModel(
            DtoConverters.toV3Cluster(this.persistenceService.getCluster(id))
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
    public PagedModel<EntityModel<Cluster>> getClusters(
        @RequestParam(value = "name", required = false) @Nullable final String name,
        @RequestParam(value = "status", required = false) @Nullable final Set<String> statuses,
        @RequestParam(value = "tag", required = false) @Nullable final Set<String> tags,
        @RequestParam(value = "minUpdateTime", required = false) @Nullable final Long minUpdateTime,
        @RequestParam(value = "maxUpdateTime", required = false) @Nullable final Long maxUpdateTime,
        @PageableDefault(size = 64, sort = {"updated"}, direction = Sort.Direction.DESC) final Pageable page,
        final PagedResourcesAssembler<Cluster> assembler
    ) throws GenieException {
        log.info(
            "[getClusters] Called to find clusters [name | statuses | tags | minUpdateTime | maxUpdateTime | page]\n"
                + "{} | {} | {} | {} | {} | {}",
            name,
            statuses,
            tags,
            minUpdateTime,
            maxUpdateTime,
            page
        );
        //Create this conversion internal in case someone uses lower case by accident?
        Set<ClusterStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(ClusterStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(
                    DtoConverters.toV4ClusterStatus(com.netflix.genie.common.dto.ClusterStatus.parse(status))
                );
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
                            clusterList.add(DtoConverters.toV3Cluster(this.persistenceService.getCluster(id)));
                        } catch (final NotFoundException ge) {
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

                clusters = this.persistenceService
                    .findClusters(
                        finalName.orElse(null),
                        enumStatuses,
                        finalTags,
                        minUpdateTime == null ? null : Instant.ofEpochMilli(minUpdateTime),
                        maxUpdateTime == null ? null : Instant.ofEpochMilli(maxUpdateTime),
                        page
                    )
                    .map(DtoConverters::toV3Cluster);
            } else {
                clusters = this.persistenceService
                    .findClusters(
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
            clusters = this.persistenceService
                .findClusters(
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
        final Link self = WebMvcLinkBuilder
            .linkTo(
                WebMvcLinkBuilder
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

        return assembler.toModel(clusters, this.clusterModelAssembler, self);
    }

    /**
     * Update a cluster configuration.
     *
     * @param id            unique if for cluster to update
     * @param updateCluster contains the cluster information to update
     * @throws NotFoundException           If no cluster with {@literal id} exists
     * @throws PreconditionFailedException If the ids don't match
     */
    @PutMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateCluster(
        @PathVariable("id") final String id,
        @RequestBody final Cluster updateCluster
    ) throws NotFoundException, PreconditionFailedException {
        log.info("[updateCluster] Called with id {} update fields {}", id, updateCluster);
        this.persistenceService.updateCluster(id, DtoConverters.toV4Cluster(updateCluster));
    }

    /**
     * Patch a cluster using JSON Patch.
     *
     * @param id    The id of the cluster to patch
     * @param patch The JSON Patch instructions
     * @throws NotFoundException           If no cluster with {@literal id} exists
     * @throws PreconditionFailedException If the ids don't match
     * @throws GenieServerException        If the patch can't be applied
     */
    @PatchMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void patchCluster(
        @PathVariable("id") final String id,
        @RequestBody final JsonPatch patch
    ) throws NotFoundException, PreconditionFailedException, GenieServerException {
        log.info("[patchCluster] Called with id {} with patch {}", id, patch);

        final Cluster currentCluster = DtoConverters.toV3Cluster(this.persistenceService.getCluster(id));

        try {
            log.debug("Will patch cluster {}. Original state: {}", id, currentCluster);
            final JsonNode clusterNode = GenieObjectMapper.getMapper().valueToTree(currentCluster);
            final JsonNode postPatchNode = patch.apply(clusterNode);
            final Cluster patchedCluster = GenieObjectMapper.getMapper().treeToValue(postPatchNode, Cluster.class);
            log.debug("Finished patching cluster {}. New state: {}", id, patchedCluster);
            this.persistenceService.updateCluster(id, DtoConverters.toV4Cluster(patchedCluster));
        } catch (final JsonPatchException | IOException e) {
            log.error("Unable to patch cluster {} with patch {} due to exception.", id, patch, e);
            throw new GenieServerException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Delete a cluster configuration.
     *
     * @param id unique id for cluster to delete
     * @throws PreconditionFailedException If the cluster can't be deleted due to constraints
     */
    @DeleteMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteCluster(@PathVariable("id") final String id) throws PreconditionFailedException {
        log.info("[deleteCluster] Called for id: {}", id);
        this.persistenceService.deleteCluster(id);
    }

    /**
     * Delete all clusters from database.
     *
     * @throws PreconditionFailedException If any cluster can't be deleted due to a constraint in the system
     */
    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteAllClusters() throws PreconditionFailedException {
        log.warn("[deleteAllClusters] Called");
        this.persistenceService.deleteAllClusters();
    }

    /**
     * Add new configuration files to a given cluster.
     *
     * @param id      The id of the cluster to add the configuration file to. Not null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @PostMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addConfigsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws NotFoundException {
        log.info("[addConfigsForCluster] Called with id {} and config {}", id, configs);
        this.persistenceService.addConfigsToResource(
            id,
            configs,
            com.netflix.genie.common.external.dtos.v4.Cluster.class
        );
    }

    /**
     * Get all the configuration files for a given cluster.
     *
     * @param id The id of the cluster to get the configuration files for. Not NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @GetMapping(value = "/{id}/configs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getConfigsForCluster(@PathVariable("id") final String id) throws NotFoundException {
        log.info("[getConfigsForCluster] Called with id {}", id);
        return this.persistenceService.getConfigsForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Cluster.class
        );
    }

    /**
     * Update the configuration files for a given cluster.
     *
     * @param id      The id of the cluster to update the configuration files for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration files with. Not null/empty/blank.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @PutMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateConfigsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws NotFoundException {
        log.info("[updateConfigsForCluster] Called with id {} and configs {}", id, configs);
        this.persistenceService.updateConfigsForResource(
            id,
            configs,
            com.netflix.genie.common.external.dtos.v4.Cluster.class
        );
    }

    /**
     * Delete the all configuration files from a given cluster.
     *
     * @param id The id of the cluster to delete the configuration files from. Not null/empty/blank.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @DeleteMapping(value = "/{id}/configs")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllConfigsForCluster(@PathVariable("id") final String id) throws NotFoundException {
        log.info("[removeAllConfigsForCluster] Called with id {}", id);
        this.persistenceService.removeAllConfigsForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Cluster.class
        );
    }

    /**
     * Add new dependency files for a given cluster.
     *
     * @param id           The id of the cluster to add the dependency file to. Not null/empty/blank.
     * @param dependencies The dependency files to add. Not null.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @PostMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addDependenciesForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws NotFoundException {
        log.info("[addDependenciesForCluster] Called with id {} and dependencies {}", id, dependencies);
        this.persistenceService.addDependenciesToResource(
            id,
            dependencies,
            com.netflix.genie.common.external.dtos.v4.Cluster.class
        );
    }

    /**
     * Get all the dependency files for a given cluster.
     *
     * @param id The id of the cluster to get the dependency files for. Not NULL/empty/blank
     * @return The set of dependency files
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @GetMapping(value = "/{id}/dependencies", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getDependenciesForCluster(@PathVariable("id") final String id) throws NotFoundException {
        log.info("[getDependenciesForCluster] Called with id {}", id);
        return this.persistenceService.getDependenciesForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Cluster.class
        );
    }

    /**
     * Update the dependency files for a given cluster.
     *
     * @param id           The id of the cluster to update the dependency files for. Not null/empty/blank.
     * @param dependencies The dependency files to replace existing dependency files with. Not null/empty/blank.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @PutMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateDependenciesForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws NotFoundException {
        log.info("[updateDependenciesForCluster] Called with id {} and dependencies {}", id, dependencies);
        this.persistenceService.updateDependenciesForResource(
            id,
            dependencies,
            com.netflix.genie.common.external.dtos.v4.Cluster.class
        );
    }

    /**
     * Delete the all dependency files from a given cluster.
     *
     * @param id The id of the cluster to delete the dependency files from. Not null/empty/blank.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @DeleteMapping(value = "/{id}/dependencies")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllDependenciesForCluster(@PathVariable("id") final String id) throws NotFoundException {
        log.info("[removeAllDependenciesForCluster] Called with id {}", id);
        this.persistenceService.removeAllDependenciesForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Cluster.class
        );
    }

    /**
     * Add new tags to a given cluster.
     *
     * @param id   The id of the cluster to add the tags to. Not null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @PostMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addTagsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws NotFoundException {
        log.info("[addTagsForCluster] Called with id {} and tags {}", id, tags);
        this.persistenceService.addTagsToResource(id, tags, com.netflix.genie.common.external.dtos.v4.Cluster.class);
    }

    /**
     * Get all the tags for a given cluster.
     *
     * @param id The id of the cluster to get the tags for. Not NULL/empty/blank.
     * @return The active set of tags.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @GetMapping(value = "/{id}/tags", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getTagsForCluster(@PathVariable("id") final String id) throws NotFoundException {
        log.info("[getTagsForCluster] Called with id {}", id);
        // Left this way for v3 tag conversion
        return DtoConverters.toV3Cluster(this.persistenceService.getCluster(id)).getTags();
    }

    /**
     * Update the tags for a given cluster.
     *
     * @param id   The id of the cluster to update the tags for. Not null/empty/blank.
     * @param tags The tags to replace existing configuration files with. Not null/empty/blank.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @PutMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateTagsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws NotFoundException {
        log.info("[updateTagsForCluster] Called with id {} and tags {}", id, tags);
        this.persistenceService.updateTagsForResource(
            id,
            tags,
            com.netflix.genie.common.external.dtos.v4.Cluster.class
        );
    }

    /**
     * Delete the all tags from a given cluster.
     *
     * @param id The id of the cluster to delete the tags from. Not null/empty/blank.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @DeleteMapping(value = "/{id}/tags")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllTagsForCluster(@PathVariable("id") final String id) throws NotFoundException {
        log.info("[removeAllTagsForCluster] Called with id {}", id);
        this.persistenceService.removeAllTagsForResource(id, com.netflix.genie.common.external.dtos.v4.Cluster.class);
    }

    /**
     * Remove an tag from a given cluster.
     *
     * @param id  The id of the cluster to delete the tag from. Not null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @throws NotFoundException If no cluster with {@literal id} exists
     */
    @DeleteMapping(value = "/{id}/tags/{tag}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeTagForCluster(
        @PathVariable("id") final String id,
        @PathVariable("tag") final String tag
    ) throws NotFoundException {
        log.info("[removeTagForCluster] Called with id {} and tag {}", id, tag);
        this.persistenceService.removeTagForResource(id, tag, com.netflix.genie.common.external.dtos.v4.Cluster.class);
    }

    /**
     * Add new commandIds to the given cluster. This is a no-op as of 4.0.0.
     *
     * @param id         The id of the cluster to add the commandIds to. Not null/empty/blank.
     * @param commandIds The ids of the commandIds to add. Not null.
     */
    @PostMapping(value = "/{id}/commands", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addCommandsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final List<String> commandIds
    ) {
        log.info("[addCommandsForCluster] Called with id {} and commandIds {}. No-op.", id, commandIds);
    }

    /**
     * Get all the commands configured for a given cluster. This is a no-op as of 4.0.0.
     *
     * @param id       The id of the cluster to get the command files for. Not NULL/empty/blank.
     * @param statuses The various statuses to return commandIds for.
     * @return The active set of commandIds for the cluster.
     * @throws NotFoundException If the cluster doesn't exist
     */
    @GetMapping(value = "/{id}/commands", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public List<EntityModel<Command>> getCommandsForCluster(
        @PathVariable("id") final String id,
        @RequestParam(value = "status", required = false) @Nullable final Set<String> statuses
    ) throws NotFoundException {
        log.info("[getCommandsForCluster] Called with id {} status {}. No-op.", id, statuses);
        // Keep the contract where if the cluster doesn't exist a 404 is thrown. May be slightly slower but
        // more accurate.
        this.persistenceService.getCluster(id);
        return EMPTY_COMMAND_LIST;
    }

    /**
     * Set the commandIds for a given cluster. This is a no-op as of 4.0.0.
     *
     * @param id         The id of the cluster to update the configuration files for. Not null/empty/blank.
     * @param commandIds The ids of the commands to replace existing commands with. Not null/empty/blank.
     */
    @PutMapping(value = "/{id}/commands", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void setCommandsForCluster(
        @PathVariable("id") final String id,
        @RequestBody final List<String> commandIds
    ) {
        log.info("[setCommandsForCluster] Called with id {} and commandIds {}. No-op.", id, commandIds);
    }

    /**
     * Remove the all commandIds from a given cluster. This is a no-op as of 4.0.0.
     *
     * @param id The id of the cluster to delete the commandIds from. Not null/empty/blank.
     */
    @DeleteMapping(value = "/{id}/commands")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllCommandsForCluster(@PathVariable("id") final String id) {
        log.info("[removeAllCommandsForCluster] Called with id {}. No-op.", id);
    }

    /**
     * Remove a command from a given cluster. This is a no-op as of 4.0.0.
     *
     * @param id        The id of the cluster to delete the command from. Not null/empty/blank.
     * @param commandId The id of the command to remove. Not null/empty/blank.
     */
    @DeleteMapping(value = "/{id}/commands/{commandId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeCommandForCluster(
        @PathVariable("id") final String id,
        @PathVariable("commandId") final String commandId
    ) {
        log.info("[removeCommandForCluster] Called with id {} and command id {}. No-op.", id, commandId);
    }
}
