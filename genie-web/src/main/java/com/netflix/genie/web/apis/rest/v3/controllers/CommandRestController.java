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
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.external.dtos.v4.ClusterStatus;
import com.netflix.genie.common.external.dtos.v4.CommandStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ResolvedResources;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.dtos.v4.converters.DtoConverters;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ApplicationModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ClusterModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.CommandModelAssembler;
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
import javax.validation.constraints.Min;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * REST end-point for supporting commands.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@RestController
@RequestMapping(value = "/api/v3/commands")
@Slf4j
public class CommandRestController {

    private final PersistenceService persistenceService;
    private final CommandModelAssembler commandModelAssembler;
    private final ApplicationModelAssembler applicationModelAssembler;
    private final ClusterModelAssembler clusterModelAssembler;

    /**
     * Constructor.
     *
     * @param dataServices          The {@link DataServices} encapsulation instance to use
     * @param entityModelAssemblers The encapsulation of all available V3 resource assemblers
     */
    @Autowired
    public CommandRestController(final DataServices dataServices, final EntityModelAssemblers entityModelAssemblers) {
        this.persistenceService = dataServices.getPersistenceService();
        this.commandModelAssembler = entityModelAssemblers.getCommandModelAssembler();
        this.applicationModelAssembler = entityModelAssemblers.getApplicationModelAssembler();
        this.clusterModelAssembler = entityModelAssemblers.getClusterModelAssembler();
    }

    /**
     * Create a Command configuration.
     *
     * @param command The command configuration to create
     * @return The command created
     * @throws IdAlreadyExistsException When the command id was already used
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<Void> createCommand(
        @RequestBody @Valid final Command command
    ) throws IdAlreadyExistsException {
        log.info("Called to create new command {}", command);
        final String id = this.persistenceService.saveCommand(DtoConverters.toV4CommandRequest(command));
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
     * Get Command configuration for given id.
     *
     * @param id unique id for command configuration
     * @return The command configuration
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @GetMapping(value = "/{id}", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public EntityModel<Command> getCommand(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called to get command with id {}", id);
        return this.commandModelAssembler.toModel(
            DtoConverters.toV3Command(this.persistenceService.getCommand(id))
        );
    }

    /**
     * Get Command configuration based on user parameters.
     *
     * @param name      Name for command (optional)
     * @param user      The user who created the configuration (optional)
     * @param statuses  The statuses of the commands to get (optional)
     * @param tags      The set of tags you want the command for.
     * @param page      The page to get
     * @param assembler The paged resources assembler to use
     * @return All the Commands matching the criteria or all if no criteria
     * @throws GenieException For any error
     */
    @GetMapping(produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public PagedModel<EntityModel<Command>> getCommands(
        @RequestParam(value = "name", required = false) @Nullable final String name,
        @RequestParam(value = "user", required = false) @Nullable final String user,
        @RequestParam(value = "status", required = false) @Nullable final Set<String> statuses,
        @RequestParam(value = "tag", required = false) @Nullable final Set<String> tags,
        @PageableDefault(size = 64, sort = {"updated"}, direction = Sort.Direction.DESC) final Pageable page,
        final PagedResourcesAssembler<Command> assembler
    ) throws GenieException {
        log.info(
            "Called [name | user | status | tags | page]\n{} | {} | {} | {} | {}",
            name,
            user,
            statuses,
            tags,
            page
        );

        Set<CommandStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(CommandStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(
                    DtoConverters.toV4CommandStatus(com.netflix.genie.common.dto.CommandStatus.parse(status))
                );
            }
        }

        final Page<Command> commands;
        if (tags != null && tags.stream().filter(tag -> tag.startsWith(DtoConverters.GENIE_ID_PREFIX)).count() >= 1L) {
            // TODO: This doesn't take into account others as compounded find...not sure if good or bad
            final List<Command> commandList = Lists.newArrayList();
            final int prefixLength = DtoConverters.GENIE_ID_PREFIX.length();
            tags
                .stream()
                .filter(tag -> tag.startsWith(DtoConverters.GENIE_ID_PREFIX))
                .forEach(
                    tag -> {
                        final String id = tag.substring(prefixLength);
                        try {
                            commandList.add(DtoConverters.toV3Command(this.persistenceService.getCommand(id)));
                        } catch (final NotFoundException ge) {
                            log.debug("No command with id {} found", id, ge);
                        }
                    }
                );
            commands = new PageImpl<>(commandList);
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

                commands = this.persistenceService
                    .findCommands(
                        finalName.orElse(null),
                        user,
                        enumStatuses,
                        finalTags,
                        page
                    )
                    .map(DtoConverters::toV3Command);
            } else {
                commands = this.persistenceService
                    .findCommands(
                        name,
                        user,
                        enumStatuses,
                        finalTags,
                        page
                    )
                    .map(DtoConverters::toV3Command);
            }
        } else {
            commands = this.persistenceService
                .findCommands(
                    name,
                    user,
                    enumStatuses,
                    tags,
                    page
                )
                .map(DtoConverters::toV3Command);
        }

        // Build the self link which will be used for the next, previous, etc links
        final Link self = WebMvcLinkBuilder
            .linkTo(
                WebMvcLinkBuilder
                    .methodOn(CommandRestController.class)
                    .getCommands(
                        name,
                        user,
                        statuses,
                        tags,
                        page,
                        assembler
                    )
            ).withSelfRel();

        return assembler.toModel(
            commands,
            this.commandModelAssembler,
            self
        );
    }

    /**
     * Update command configuration.
     *
     * @param id            unique id for the configuration to update.
     * @param updateCommand the information to update the command with
     * @throws NotFoundException           When no {@link Command} with the given {@literal id} exists
     * @throws PreconditionFailedException When {@literal id} and the {@literal updateCommand} id don't match
     */
    @PutMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateCommand(
        @PathVariable("id") final String id,
        @RequestBody final Command updateCommand
    ) throws NotFoundException, PreconditionFailedException {
        log.debug("Called to update command {}", updateCommand);
        this.persistenceService.updateCommand(id, DtoConverters.toV4Command(updateCommand));
    }

    /**
     * Patch a command using JSON Patch.
     *
     * @param id    The id of the command to patch
     * @param patch The JSON Patch instructions
     * @throws NotFoundException           When no {@link Command} with the given {@literal id} exists
     * @throws PreconditionFailedException When {@literal id} and the {@literal updateCommand} id don't match
     * @throws GenieServerException        When the patch can't be applied
     */
    @PatchMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void patchCommand(
        @PathVariable("id") final String id,
        @RequestBody final JsonPatch patch
    ) throws NotFoundException, PreconditionFailedException, GenieServerException {
        log.info("Called to patch command {} with patch {}", id, patch);

        final Command currentCommand = DtoConverters.toV3Command(this.persistenceService.getCommand(id));

        try {
            log.debug("Will patch cluster {}. Original state: {}", id, currentCommand);
            final JsonNode commandNode = GenieObjectMapper.getMapper().valueToTree(currentCommand);
            final JsonNode postPatchNode = patch.apply(commandNode);
            final Command patchedCommand = GenieObjectMapper.getMapper().treeToValue(postPatchNode, Command.class);
            log.debug("Finished patching command {}. New state: {}", id, patchedCommand);
            this.persistenceService.updateCommand(id, DtoConverters.toV4Command(patchedCommand));
        } catch (final JsonPatchException | IOException e) {
            log.error("Unable to patch command {} with patch {} due to exception.", id, patch, e);
            throw new GenieServerException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Delete all applications from database.
     *
     * @throws PreconditionFailedException When deleting one of the commands would violated a consistency issue
     */
    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteAllCommands() throws PreconditionFailedException {
        log.warn("Called to delete all commands.");
        this.persistenceService.deleteAllCommands();
    }

    /**
     * Delete a command.
     *
     * @param id unique id for configuration to delete
     * @throws NotFoundException If no {@link Command} exists with the given {@literal id}
     */
    @DeleteMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteCommand(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called to delete command with id {}", id);
        this.persistenceService.deleteCommand(id);
    }

    /**
     * Add new configuration files to a given command.
     *
     * @param id      The id of the command to add the configuration file to. Not null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @PostMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addConfigsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws NotFoundException {
        log.info("Called with id {} and config {}", id, configs);
        this.persistenceService.addConfigsToResource(
            id,
            configs,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Get all the configuration files for a given command.
     *
     * @param id The id of the command to get the configuration files for. Not NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @GetMapping(value = "/{id}/configs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getConfigsForCommand(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        return this.persistenceService.getConfigsForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Update the configuration files for a given command.
     *
     * @param id      The id of the command to update the configuration files for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration files with. Not null/empty/blank.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @PutMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateConfigsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws NotFoundException {
        log.info("Called with id {} and configs {}", id, configs);
        this.persistenceService.updateConfigsForResource(
            id,
            configs,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Delete the all configuration files from a given command.
     *
     * @param id The id of the command to delete the configuration files from. Not null/empty/blank.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @DeleteMapping(value = "/{id}/configs")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllConfigsForCommand(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        this.persistenceService.removeAllConfigsForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Add new dependency files for a given command.
     *
     * @param id           The id of the command to add the dependency file to. Not null/empty/blank.
     * @param dependencies The dependency files to add. Not null.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @PostMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addDependenciesForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws NotFoundException {
        log.info("Called with id {} and dependencies {}", id, dependencies);
        this.persistenceService.addDependenciesToResource(
            id,
            dependencies,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Get all the dependency files for a given command.
     *
     * @param id The id of the command to get the dependency files for. Not NULL/empty/blank.
     * @return The set of dependency files.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @GetMapping(value = "/{id}/dependencies", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getDependenciesForCommand(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        return this.persistenceService.getDependenciesForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Update the dependency files for a given command.
     *
     * @param id           The id of the command to update the dependency files for. Not null/empty/blank.
     * @param dependencies The dependency files to replace existing dependency files with. Not null/empty/blank.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @PutMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateDependenciesForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws NotFoundException {
        log.info("Called with id {} and dependencies {}", id, dependencies);
        this.persistenceService.updateDependenciesForResource(
            id,
            dependencies,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Delete the all dependency files from a given command.
     *
     * @param id The id of the command to delete the dependency files from. Not null/empty/blank.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @DeleteMapping(value = "/{id}/dependencies")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllDependenciesForCommand(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        this.persistenceService.removeAllDependenciesForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Add new tags to a given command.
     *
     * @param id   The id of the command to add the tags to. Not null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @PostMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addTagsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws NotFoundException {
        log.info("Called with id {} and tags {}", id, tags);
        this.persistenceService.addTagsToResource(
            id,
            tags,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Get all the tags for a given command.
     *
     * @param id The id of the command to get the tags for. Not NULL/empty/blank.
     * @return The active set of tags.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @GetMapping(value = "/{id}/tags", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getTagsForCommand(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        // Kept this way for V3 tag conversion
        return DtoConverters.toV3Command(this.persistenceService.getCommand(id)).getTags();
    }

    /**
     * Update the tags for a given command.
     *
     * @param id   The id of the command to update the tags for. Not null/empty/blank.
     * @param tags The tags to replace existing tags with. Not null/empty/blank.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @PutMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateTagsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws NotFoundException {
        log.info("Called with id {} and tags {}", id, tags);
        this.persistenceService.updateTagsForResource(
            id,
            tags,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Delete the all tags from a given command.
     *
     * @param id The id of the command to delete the tags from. Not null/empty/blank.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @DeleteMapping(value = "/{id}/tags")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllTagsForCommand(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        this.persistenceService.removeAllTagsForResource(id, com.netflix.genie.common.external.dtos.v4.Command.class);
    }

    /**
     * Remove an tag from a given command.
     *
     * @param id  The id of the command to delete the tag from. Not null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @DeleteMapping(value = "/{id}/tags/{tag}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeTagForCommand(
        @PathVariable("id") final String id,
        @PathVariable("tag") final String tag
    ) throws NotFoundException {
        log.info("Called with id {} and tag {}", id, tag);
        this.persistenceService.removeTagForResource(
            id,
            tag,
            com.netflix.genie.common.external.dtos.v4.Command.class
        );
    }

    /**
     * Add applications for the given command.
     *
     * @param id             The id of the command to add the applications to. Not null/empty/blank.
     * @param applicationIds The ids of the applications to add. Not null. No duplicates
     * @throws NotFoundException           When no {@link Command} with the given {@literal id} exists
     * @throws PreconditionFailedException If there are any duplicate applications in the list or when combined with
     *                                     existing applications associated with the command
     */
    @PostMapping(value = "/{id}/applications", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addApplicationsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final List<String> applicationIds
    ) throws NotFoundException, PreconditionFailedException {
        log.info("Called with id {} and application {}", id, applicationIds);
        this.persistenceService.addApplicationsForCommand(id, applicationIds);
    }

    /**
     * Get the applications configured for a given command.
     *
     * @param id The id of the command to get the application files for. Not NULL/empty/blank.
     * @return The active applications for the command.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @GetMapping(value = "/{id}/applications", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public List<EntityModel<Application>> getApplicationsForCommand(
        @PathVariable("id") final String id
    ) throws NotFoundException {
        log.info("Called with id {}", id);
        return this.persistenceService.getApplicationsForCommand(id)
            .stream()
            .map(DtoConverters::toV3Application)
            .map(this.applicationModelAssembler::toModel)
            .collect(Collectors.toList());
    }

    /**
     * Set the applications for the given command.
     *
     * @param id             The id of the command to add the applications to. Not null/empty/blank.
     * @param applicationIds The ids of the applications to set in order. Not null.
     * @throws NotFoundException           When no {@link Command} with the given {@literal id} exists
     * @throws PreconditionFailedException If there are any duplicate applications in the list
     */
    @PutMapping(value = "/{id}/applications", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void setApplicationsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final List<String> applicationIds
    ) throws NotFoundException, PreconditionFailedException {
        log.info("Called with id {} and application {}", id, applicationIds);
        this.persistenceService.setApplicationsForCommand(id, applicationIds);
    }

    /**
     * Remove the applications from a given command.
     *
     * @param id The id of the command to delete the applications from. Not null/empty/blank.
     * @throws NotFoundException           When no {@link Command} with the given {@literal id} exists
     * @throws PreconditionFailedException If constraints block removal
     */
    @DeleteMapping(value = "/{id}/applications")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllApplicationsForCommand(
        @PathVariable("id") final String id
    ) throws NotFoundException, PreconditionFailedException {
        log.info("Called with id '{}'", id);
        this.persistenceService.removeApplicationsForCommand(id);
    }

    /**
     * Remove the application from a given command.
     *
     * @param id    The id of the command to delete the application from. Not null/empty/blank.
     * @param appId The id of the application to remove from the command. Not null/empty/blank.
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists or no application exists
     */
    @DeleteMapping(value = "/{id}/applications/{appId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeApplicationForCommand(
        @PathVariable("id") final String id,
        @PathVariable("appId") final String appId
    ) throws NotFoundException {
        log.info("Called with id '{}' and app id {}", id, appId);
        this.persistenceService.removeApplicationForCommand(id, appId);
    }

    /**
     * Get all the clusters this command is associated with.
     *
     * @param id       The id of the command to get the clusters for. Not NULL/empty/blank.
     * @param statuses The statuses of the clusters to get
     * @return The list of clusters
     * @throws NotFoundException          When no {@link Command} with the given {@literal id} exists
     * @throws GeniePreconditionException If a supplied status is not valid
     */
    @GetMapping(value = "/{id}/clusters", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<EntityModel<Cluster>> getClustersForCommand(
        @PathVariable("id") final String id,
        @RequestParam(value = "status", required = false) @Nullable final Set<String> statuses
    ) throws NotFoundException, GeniePreconditionException {
        log.info("Called with id {} and statuses {}", id, statuses);

        Set<ClusterStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(ClusterStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(
                    DtoConverters.toV4ClusterStatus(com.netflix.genie.common.dto.ClusterStatus.parse(status))
                );
            }
        }

        return this.persistenceService.getClustersForCommand(id, enumStatuses)
            .stream()
            .map(DtoConverters::toV3Cluster)
            .map(this.clusterModelAssembler::toModel)
            .collect(Collectors.toSet());
    }

    /**
     * Get all the {@link Criterion} currently associated with the command in priority order.
     *
     * @param id The id of the command to get the criteria for
     * @return The criteria
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @GetMapping(value = "/{id}/clusterCriteria", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public List<Criterion> getClusterCriteriaForCommand(
        @PathVariable("id") final String id
    ) throws NotFoundException {
        log.info("Called for command {}", id);
        return this.persistenceService.getClusterCriteriaForCommand(id);
    }

    /**
     * Remove all the {@link Criterion} currently associated with the command.
     *
     * @param id The id of the command to remove the criteria for
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @DeleteMapping(value = "/{id}/clusterCriteria")
    @ResponseStatus(HttpStatus.OK)
    public void removeAllClusterCriteriaFromCommand(
        @PathVariable("id") final String id
    ) throws NotFoundException {
        log.info("Called for command {}", id);
        this.persistenceService.removeAllClusterCriteriaForCommand(id);
    }

    /**
     * Add a new {@link Criterion} as the lowest priority criterion for the given command.
     *
     * @param id        The id of the command to add the new criterion to
     * @param criterion The {@link Criterion} to add
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @PostMapping(value = "/{id}/clusterCriteria", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public void addClusterCriterionForCommand(
        @PathVariable("id") final String id,
        @RequestBody @Valid final Criterion criterion
    ) throws NotFoundException {
        log.info("Called to add {} as the lowest priority cluster criterion for command {}", criterion, id);
        this.persistenceService.addClusterCriterionForCommand(id, criterion);
    }

    /**
     * Set all new cluster criteria for the given command.
     *
     * @param id              The id of the command to add the new criteria to
     * @param clusterCriteria The list of {@link Criterion} in priority order to set for the given command
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @PutMapping(value = "/{id}/clusterCriteria", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public void setClusterCriteriaForCommand(
        @PathVariable("id") final String id,
        @RequestBody @Valid final List<Criterion> clusterCriteria
    ) throws NotFoundException {
        log.info("Called to set {} as the cluster criteria for command {}", clusterCriteria, id);
        this.persistenceService.setClusterCriteriaForCommand(id, clusterCriteria);
    }

    /**
     * Insert a new cluster criterion for the given command at the supplied priority.
     *
     * @param id        The id of the command to add the new criterion for
     * @param priority  The priority (min 0) to insert the criterion at in the list
     * @param criterion The {@link Criterion} to add
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @PutMapping(value = "/{id}/clusterCriteria/{priority}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public void insertClusterCriterionForCommand(
        @PathVariable("id") final String id,
        @PathVariable("priority") @Min(0) final int priority,
        @RequestBody @Valid final Criterion criterion
    ) throws NotFoundException {
        log.info("Called to insert new criterion {} for command {} with priority {}", criterion, id, priority);
        this.persistenceService.addClusterCriterionForCommand(id, criterion, priority);
    }

    /**
     * Remove the criterion with the given priority from the given command.
     *
     * @param id       The id of the command to remove the criterion from
     * @param priority The priority (min 0, max number of existing criteria minus one) of the criterion to remove
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @DeleteMapping(value = "/{id}/clusterCriteria/{priority}")
    @ResponseStatus(HttpStatus.OK)
    public void removeClusterCriterionFromCommand(
        @PathVariable("id") final String id,
        @PathVariable("priority") @Min(0) final int priority
    ) throws NotFoundException {
        log.info("Called to remove the criterion from command {} with priority {}", id, priority);
        this.persistenceService.removeClusterCriterionForCommand(id, priority);
    }

    /**
     * For a given {@link Command} retrieve the {@link Criterion} and attempt to resolve all the {@link Cluster}'s the
     * criteria would currently match within the database.
     *
     * @param id               The id of the command to get the criterion from
     * @param addDefaultStatus Whether the system should add the default {@link ClusterStatus} to the {@link Criterion}
     *                         if no status currently is within the {@link Criterion}. The default value is
     *                         {@literal true} which will currently add the status {@link ClusterStatus#UP}
     * @return A list {@link ResolvedResources} of each criterion to the {@link Cluster}'s it resolved to
     * @throws NotFoundException When no {@link Command} with the given {@literal id} exists
     */
    @GetMapping(value = "/{id}/resolvedClusters", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public List<ResolvedResources<Cluster>> resolveClustersForCommandClusterCriteria(
        @PathVariable("id") final String id,
        @RequestParam(value = "addDefaultStatus", defaultValue = "true") final Boolean addDefaultStatus
    ) throws NotFoundException {
        log.info("Called to resolve clusters for command {}", id);

        final List<Criterion> criteria = this.persistenceService.getClusterCriteriaForCommand(id);

        final List<ResolvedResources<Cluster>> resolvedResources = Lists.newArrayList();
        for (final Criterion criterion : criteria) {
            resolvedResources.add(
                new ResolvedResources<>(
                    criterion,
                    this.persistenceService
                        .findClustersMatchingCriterion(criterion, addDefaultStatus)
                        .stream()
                        .map(DtoConverters::toV3Cluster)
                        .collect(Collectors.toSet())
                )
            );
        }
        return resolvedResources;
    }
}
