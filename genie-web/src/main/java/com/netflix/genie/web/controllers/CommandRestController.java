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
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.hateoas.assemblers.ApplicationResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.ClusterResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.CommandResourceAssembler;
import com.netflix.genie.web.hateoas.resources.ApplicationResource;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import com.netflix.genie.web.hateoas.resources.CommandResource;
import com.netflix.genie.web.services.CommandPersistenceService;
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

    private final CommandPersistenceService commandPersistenceService;
    private final CommandResourceAssembler commandResourceAssembler;
    private final ApplicationResourceAssembler applicationResourceAssembler;
    private final ClusterResourceAssembler clusterResourceAssembler;

    /**
     * Constructor.
     *
     * @param commandPersistenceService    The command configuration service to use.
     * @param commandResourceAssembler     The assembler to use to convert commands to command HAL resources
     * @param applicationResourceAssembler The assembler to use to convert applications to application HAL resources
     * @param clusterResourceAssembler     The assembler to use to convert clusters to cluster HAL resources
     */
    @Autowired
    public CommandRestController(
        final CommandPersistenceService commandPersistenceService,
        final CommandResourceAssembler commandResourceAssembler,
        final ApplicationResourceAssembler applicationResourceAssembler,
        final ClusterResourceAssembler clusterResourceAssembler
    ) {
        this.commandPersistenceService = commandPersistenceService;
        this.commandResourceAssembler = commandResourceAssembler;
        this.applicationResourceAssembler = applicationResourceAssembler;
        this.clusterResourceAssembler = clusterResourceAssembler;
    }

    /**
     * Create a Command configuration.
     *
     * @param command The command configuration to create
     * @return The command created
     * @throws GenieException For any error
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<Void> createCommand(@RequestBody @Valid final Command command) throws GenieException {
        log.debug("called to create new command configuration {}", command);
        final String id = this.commandPersistenceService.createCommand(DtoConverters.toV4CommandRequest(command));
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
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public CommandResource getCommand(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called to get command with id {}", id);
        return this.commandResourceAssembler.toResource(
            DtoConverters.toV3Command(this.commandPersistenceService.getCommand(id))
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
    public PagedResources<CommandResource> getCommands(
        @RequestParam(value = "name", required = false) final String name,
        @RequestParam(value = "user", required = false) final String user,
        @RequestParam(value = "status", required = false) final Set<String> statuses,
        @RequestParam(value = "tag", required = false) final Set<String> tags,
        @PageableDefault(size = 64, sort = {"updated"}, direction = Sort.Direction.DESC) final Pageable page,
        final PagedResourcesAssembler<Command> assembler
    ) throws GenieException {
        log.debug("Called [name | user | status | tags | page]");
        log.debug("{} | {} | {} | {} | {}", name, user, statuses, tags, page);

        Set<CommandStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(CommandStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(CommandStatus.parse(status));
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
                            commandList.add(DtoConverters.toV3Command(this.commandPersistenceService.getCommand(id)));
                        } catch (final GenieException ge) {
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

                commands = this.commandPersistenceService
                    .getCommands(
                        finalName.orElse(null),
                        user,
                        enumStatuses,
                        finalTags,
                        page
                    )
                    .map(DtoConverters::toV3Command);
            } else {
                commands = this.commandPersistenceService
                    .getCommands(
                        name,
                        user,
                        enumStatuses,
                        finalTags,
                        page
                    )
                    .map(DtoConverters::toV3Command);
            }
        } else {
            commands = this.commandPersistenceService
                .getCommands(
                    name,
                    user,
                    enumStatuses,
                    tags,
                    page
                )
                .map(DtoConverters::toV3Command);
        }

        // Build the self link which will be used for the next, previous, etc links
        final Link self = ControllerLinkBuilder
            .linkTo(
                ControllerLinkBuilder
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

        return assembler.toResource(
            commands,
            this.commandResourceAssembler,
            self
        );
    }

    /**
     * Update command configuration.
     *
     * @param id            unique id for the configuration to update.
     * @param updateCommand the information to update the command with
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateCommand(
        @PathVariable("id") final String id,
        @RequestBody final Command updateCommand
    ) throws GenieException {
        log.debug("Called to update command {}", updateCommand);
        this.commandPersistenceService.updateCommand(id, DtoConverters.toV4Command(updateCommand));
    }

    /**
     * Patch a command using JSON Patch.
     *
     * @param id    The id of the command to patch
     * @param patch The JSON Patch instructions
     * @throws GenieException On error
     */
    @PatchMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void patchCommand(
        @PathVariable("id") final String id,
        @RequestBody final JsonPatch patch
    ) throws GenieException {
        log.debug("Called to patch command {} with patch {}", id, patch);

        final Command currentCommand = DtoConverters.toV3Command(this.commandPersistenceService.getCommand(id));

        try {
            log.debug("Will patch cluster {}. Original state: {}", id, currentCommand);
            final JsonNode commandNode = GenieObjectMapper.getMapper().valueToTree(currentCommand);
            final JsonNode postPatchNode = patch.apply(commandNode);
            final Command patchedCommand = GenieObjectMapper.getMapper().treeToValue(postPatchNode, Command.class);
            log.debug("Finished patching command {}. New state: {}", id, patchedCommand);
            this.commandPersistenceService.updateCommand(id, DtoConverters.toV4Command(patchedCommand));
        } catch (final JsonPatchException | IOException e) {
            log.error("Unable to patch command {} with patch {} due to exception.", id, patch, e);
            throw new GenieServerException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Delete all applications from database.
     *
     * @throws GenieException For any error
     */
    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteAllCommands() throws GenieException {
        log.debug("called to delete all commands.");
        this.commandPersistenceService.deleteAllCommands();
    }

    /**
     * Delete a command.
     *
     * @param id unique id for configuration to delete
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteCommand(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called to delete command with id {}", id);
        this.commandPersistenceService.deleteCommand(id);
    }

    /**
     * Add new configuration files to a given command.
     *
     * @param id      The id of the command to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addConfigsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws GenieException {
        log.debug("Called with id {} and config {}", id, configs);
        this.commandPersistenceService.addConfigsForCommand(id, configs);
    }

    /**
     * Get all the configuration files for a given command.
     *
     * @param id The id of the command to get the configuration files for. Not
     *           NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/configs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getConfigsForCommand(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return this.commandPersistenceService.getConfigsForCommand(id);
    }

    /**
     * Update the configuration files for a given command.
     *
     * @param id      The id of the command to update the configuration files for.
     *                Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     *                files with. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateConfigsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws GenieException {
        log.debug("Called with id {} and configs {}", id, configs);
        this.commandPersistenceService.updateConfigsForCommand(id, configs);
    }

    /**
     * Delete the all configuration files from a given command.
     *
     * @param id The id of the command to delete the configuration files from.
     *           Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/configs")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllConfigsForCommand(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        this.commandPersistenceService.removeAllConfigsForCommand(id);
    }

    /**
     * Add new dependency files for a given command.
     *
     * @param id           The id of the command to add the dependency file to. Not
     *                     null/empty/blank.
     * @param dependencies The dependency files to add. Not null.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addDependenciesForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws GenieException {
        log.debug("Called with id {} and dependencies {}", id, dependencies);
        this.commandPersistenceService.addDependenciesForCommand(id, dependencies);
    }

    /**
     * Get all the dependency files for a given command.
     *
     * @param id The id of the command to get the dependency files for. Not
     *           NULL/empty/blank.
     * @return The set of dependency files.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/dependencies", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getDependenciesForCommand(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return this.commandPersistenceService.getDependenciesForCommand(id);
    }

    /**
     * Update the dependency files for a given command.
     *
     * @param id           The id of the command to update the dependency files for. Not
     *                     null/empty/blank.
     * @param dependencies The dependency files to replace existing dependency files with. Not
     *                     null/empty/blank.
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateDependenciesForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws GenieException {
        log.debug("Called with id {} and dependencies {}", id, dependencies);
        this.commandPersistenceService.updateDependenciesForCommand(id, dependencies);
    }

    /**
     * Delete the all dependency files from a given command.
     *
     * @param id The id of the command to delete the dependency files from. Not
     *           null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/dependencies")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllDependenciesForCommand(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        this.commandPersistenceService.removeAllDependenciesForCommand(id);
    }

    /**
     * Add new tags to a given command.
     *
     * @param id   The id of the command to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addTagsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws GenieException {
        log.debug("Called with id {} and tags {}", id, tags);
        this.commandPersistenceService.addTagsForCommand(id, tags);
    }

    /**
     * Get all the tags for a given command.
     *
     * @param id The id of the command to get the tags for. Not
     *           NULL/empty/blank.
     * @return The active set of tags.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/tags", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getTagsForCommand(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return DtoConverters.toV3Command(this.commandPersistenceService.getCommand(id)).getTags();
    }

    /**
     * Update the tags for a given command.
     *
     * @param id   The id of the command to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing configuration
     *             files with. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateTagsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws GenieException {
        log.debug("Called with id {} and tags {}", id, tags);
        this.commandPersistenceService.updateTagsForCommand(id, tags);
    }

    /**
     * Delete the all tags from a given command.
     *
     * @param id The id of the command to delete the tags from.
     *           Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/tags")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllTagsForCommand(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        this.commandPersistenceService.removeAllTagsForCommand(id);
    }

    /**
     * Remove an tag from a given command.
     *
     * @param id  The id of the command to delete the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/tags/{tag}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeTagForCommand(
        @PathVariable("id") final String id,
        @PathVariable("tag") final String tag
    ) throws GenieException {
        log.debug("Called with id {} and tag {}", id, tag);
        this.commandPersistenceService.removeTagForCommand(id, tag);
    }

    /**
     * Add applications for the given command.
     *
     * @param id             The id of the command to add the applications to. Not
     *                       null/empty/blank.
     * @param applicationIds The ids of the applications to add. Not null.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/applications", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addApplicationsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final List<String> applicationIds
    ) throws GenieException {
        log.debug("Called with id {} and application {}", id, applicationIds);
        this.commandPersistenceService.addApplicationsForCommand(id, applicationIds);
    }

    /**
     * Get the applications configured for a given command.
     *
     * @param id The id of the command to get the application files for. Not
     *           NULL/empty/blank.
     * @return The active applications for the command.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/applications", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public List<ApplicationResource> getApplicationsForCommand(
        @PathVariable("id") final String id
    ) throws GenieException {
        log.debug("Called with id {}", id);
        return this.commandPersistenceService.getApplicationsForCommand(id)
            .stream()
            .map(DtoConverters::toV3Application)
            .map(this.applicationResourceAssembler::toResource)
            .collect(Collectors.toList());
    }

    /**
     * Set the applications for the given command.
     *
     * @param id             The id of the command to add the applications to. Not
     *                       null/empty/blank.
     * @param applicationIds The ids of the applications to set in order. Not null.
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}/applications", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void setApplicationsForCommand(
        @PathVariable("id") final String id,
        @RequestBody final List<String> applicationIds
    ) throws GenieException {
        log.debug("Called with id {} and application {}", id, applicationIds);
        this.commandPersistenceService.setApplicationsForCommand(id, applicationIds);
    }

    /**
     * Remove the applications from a given command.
     *
     * @param id The id of the command to delete the applications from. Not
     *           null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/applications")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllApplicationsForCommand(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id '{}'.", id);
        this.commandPersistenceService.removeApplicationsForCommand(id);
    }

    /**
     * Remove the application from a given command.
     *
     * @param id    The id of the command to delete the application from. Not
     *              null/empty/blank.
     * @param appId The id of the application to remove from the command. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/applications/{appId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeApplicationForCommand(
        @PathVariable("id") final String id,
        @PathVariable("appId") final String appId
    ) throws GenieException {
        log.debug("Called with id '{}' and app id {}", id, appId);
        this.commandPersistenceService.removeApplicationForCommand(id, appId);
    }

    /**
     * Get all the clusters this command is associated with.
     *
     * @param id       The id of the command to get the clusters for. Not
     *                 NULL/empty/blank.
     * @param statuses The statuses of the clusters to get
     * @return The list of clusters.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/clusters", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<ClusterResource> getClustersForCommand(
        @PathVariable("id") final String id,
        @RequestParam(value = "status", required = false) final Set<String> statuses
    ) throws GenieException {
        log.debug("Called with id {} and statuses {}", id, statuses);

        Set<ClusterStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(ClusterStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(ClusterStatus.parse(status));
            }
        }

        return this.commandPersistenceService.getClustersForCommand(id, enumStatuses)
            .stream()
            .map(DtoConverters::toV3Cluster)
            .map(this.clusterResourceAssembler::toResource)
            .collect(Collectors.toSet());
    }
}
