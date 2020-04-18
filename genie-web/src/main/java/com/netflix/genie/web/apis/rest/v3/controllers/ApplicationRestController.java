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
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus;
import com.netflix.genie.common.external.dtos.v4.CommandStatus;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.dtos.v4.converters.DtoConverters;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ApplicationModelAssembler;
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
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * REST end-point for supporting Applications.
 *
 * @author tgianos
 * @since 3.0.0
 */
@RestController
@RequestMapping(value = "/api/v3/applications")
@Slf4j
public class ApplicationRestController {

    private final PersistenceService persistenceService;
    private final ApplicationModelAssembler applicationModelAssembler;
    private final CommandModelAssembler commandModelAssembler;

    /**
     * Constructor.
     *
     * @param dataServices          The {@link DataServices} encapsulation instance to use
     * @param entityModelAssemblers The encapsulation of all the available V3 resource assemblers
     */
    @Autowired
    public ApplicationRestController(
        final DataServices dataServices,
        final EntityModelAssemblers entityModelAssemblers
    ) {
        this.persistenceService = dataServices.getPersistenceService();
        this.applicationModelAssembler = entityModelAssemblers.getApplicationModelAssembler();
        this.commandModelAssembler = entityModelAssemblers.getCommandModelAssembler();
    }

    /**
     * Create an Application.
     *
     * @param app The application to create
     * @return The created application configuration
     * @throws IdAlreadyExistsException If the ID was already in use
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<Void> createApplication(@RequestBody final Application app) throws IdAlreadyExistsException {
        log.info("Called to create new application: {}", app);
        final String id = this.persistenceService.saveApplication(DtoConverters.toV4ApplicationRequest(app));
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
     * Delete all applications from database.
     *
     * @throws PreconditionFailedException If any of the applications were still linked to a command
     */
    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteAllApplications() throws PreconditionFailedException {
        log.warn("Called to delete all Applications");
        this.persistenceService.deleteAllApplications();
    }

    /**
     * Get Applications based on user parameters.
     *
     * @param name      name for configuration (optional)
     * @param user      The user who created the application (optional)
     * @param statuses  The statuses of the applications (optional)
     * @param tags      The set of tags you want the application for. (optional)
     * @param type      The type of applications to get (optional)
     * @param page      The page to get
     * @param assembler The paged resources assembler to use
     * @return All applications matching the criteria
     * @throws GenieException For any error
     */
    @GetMapping(produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public PagedModel<EntityModel<Application>> getApplications(
        @RequestParam(value = "name", required = false) @Nullable final String name,
        @RequestParam(value = "user", required = false) @Nullable final String user,
        @RequestParam(value = "status", required = false) @Nullable final Set<String> statuses,
        @RequestParam(value = "tag", required = false) @Nullable final Set<String> tags,
        @RequestParam(value = "type", required = false) @Nullable final String type,
        @PageableDefault(sort = {"updated"}, direction = Sort.Direction.DESC) final Pageable page,
        final PagedResourcesAssembler<Application> assembler
    ) throws GenieException {
        log.info(
            "Finding applications [name | user | status | tags | type | pageable]\n{} | {} | {} | {} | | {} | {}",
            name,
            user,
            statuses,
            tags,
            type,
            page
        );

        Set<ApplicationStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(ApplicationStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(
                    DtoConverters.toV4ApplicationStatus(com.netflix.genie.common.dto.ApplicationStatus.parse(status))
                );
            }
        }

        final Page<Application> applications;
        if (tags != null && tags.stream().filter(tag -> tag.startsWith(DtoConverters.GENIE_ID_PREFIX)).count() >= 1L) {
            // TODO: This doesn't take into account others as compounded find...not sure if good or bad
            final List<Application> applicationList = Lists.newArrayList();
            final int prefixLength = DtoConverters.GENIE_ID_PREFIX.length();
            tags
                .stream()
                .filter(tag -> tag.startsWith(DtoConverters.GENIE_ID_PREFIX))
                .forEach(
                    tag -> {
                        final String id = tag.substring(prefixLength);
                        try {
                            applicationList.add(
                                DtoConverters.toV3Application(this.persistenceService.getApplication(id))
                            );
                        } catch (final NotFoundException ge) {
                            log.debug("No application with id {} found", id, ge);
                        }
                    }
                );
            applications = new PageImpl<>(applicationList);
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

                applications = this.persistenceService
                    .findApplications(finalName.orElse(null), user, enumStatuses, finalTags, type, page)
                    .map(DtoConverters::toV3Application);
            } else {
                applications = this.persistenceService
                    .findApplications(name, user, enumStatuses, finalTags, type, page)
                    .map(DtoConverters::toV3Application);
            }
        } else {
            applications = this.persistenceService
                .findApplications(name, user, enumStatuses, tags, type, page)
                .map(DtoConverters::toV3Application);
        }

        final Link self = WebMvcLinkBuilder.linkTo(
            WebMvcLinkBuilder
                .methodOn(ApplicationRestController.class)
                .getApplications(name, user, statuses, tags, type, page, assembler)
        ).withSelfRel();

        return assembler.toModel(
            applications,
            this.applicationModelAssembler,
            self
        );
    }

    /**
     * Get Application for given id.
     *
     * @param id unique id for application configuration
     * @return The application configuration
     * @throws NotFoundException If no application exists with the given id
     */
    @GetMapping(value = "/{id}", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public EntityModel<Application> getApplication(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called to get Application for id {}", id);
        return this.applicationModelAssembler.toModel(
            DtoConverters.toV3Application(this.persistenceService.getApplication(id))
        );
    }

    /**
     * Update application.
     *
     * @param id        unique id for configuration to update
     * @param updateApp contains the application information to update
     * @throws NotFoundException           If no application with the given id exists
     * @throws PreconditionFailedException When the id in the update doesn't match
     */
    @PutMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateApplication(
        @PathVariable("id") final String id,
        @RequestBody final Application updateApp
    ) throws NotFoundException, PreconditionFailedException {
        log.info("called to update application {} with info {}", id, updateApp);
        this.persistenceService.updateApplication(id, DtoConverters.toV4Application(updateApp));
    }

    /**
     * Patch an application using JSON Patch.
     *
     * @param id    The id of the application to patch
     * @param patch The JSON Patch instructions
     * @throws NotFoundException           If no application with the given id exists
     * @throws PreconditionFailedException When the id in the update doesn't match
     * @throws GenieServerException        If the patch can't be successfully applied
     */
    @PatchMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void patchApplication(
        @PathVariable("id") final String id,
        @RequestBody final JsonPatch patch
    ) throws NotFoundException, PreconditionFailedException, GenieServerException {
        log.info("Called to patch application {} with patch {}", id, patch);
        final Application currentApp = DtoConverters.toV3Application(this.persistenceService.getApplication(id));

        try {
            log.debug("Will patch application {}. Original state: {}", id, currentApp);
            final JsonNode applicationNode = GenieObjectMapper.getMapper().valueToTree(currentApp);
            final JsonNode postPatchNode = patch.apply(applicationNode);
            final Application patchedApp = GenieObjectMapper.getMapper().treeToValue(postPatchNode, Application.class);
            log.debug("Finished patching application {}. New state: {}", id, patchedApp);
            this.persistenceService.updateApplication(id, DtoConverters.toV4Application(patchedApp));
        } catch (final JsonPatchException | IOException e) {
            log.error("Unable to patch application {} with patch {} due to exception.", id, patch, e);
            throw new GenieServerException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Delete an application configuration from database.
     *
     * @param id unique id of configuration to delete
     * @throws PreconditionFailedException If the application is still tied to a command
     */
    @DeleteMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteApplication(@PathVariable("id") final String id) throws PreconditionFailedException {
        log.info("Delete an application with id {}", id);
        this.persistenceService.deleteApplication(id);
    }

    /**
     * Add new configuration files to a given application.
     *
     * @param id      The id of the application to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @throws NotFoundException If no application with the given id exists
     */
    @PostMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addConfigsToApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws NotFoundException {
        log.info("Called with id {} and config {}", id, configs);
        this.persistenceService.addConfigsToResource(
            id,
            configs,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Get all the configuration files for a given application.
     *
     * @param id The id of the application to get the configuration files for.
     *           Not NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws NotFoundException If no application with the given id exists
     */
    @GetMapping(value = "/{id}/configs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getConfigsForApplication(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        return this.persistenceService.getConfigsForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Update the configuration files for a given application.
     *
     * @param id      The id of the application to update the configuration files
     *                for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     *                files with. Not null/empty/blank.
     * @throws NotFoundException If no application with the given ID exists
     */
    @PutMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateConfigsForApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws NotFoundException {
        log.info("Called with id {} and configs {}", id, configs);
        this.persistenceService.updateConfigsForResource(
            id,
            configs,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Delete the all configuration files from a given application.
     *
     * @param id The id of the application to delete the configuration files
     *           from. Not null/empty/blank.
     * @throws NotFoundException If no application with the given ID exists
     */
    @DeleteMapping(value = "/{id}/configs")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllConfigsForApplication(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        this.persistenceService.removeAllConfigsForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Add new dependency files for a given application.
     *
     * @param id           The id of the application to add the dependency file to. Not
     *                     null/empty/blank.
     * @param dependencies The dependency files to add. Not null.
     * @throws NotFoundException If no application with the given ID exists
     */
    @PostMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addDependenciesForApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws NotFoundException {
        log.info("Called with id {} and dependencies {}", id, dependencies);
        this.persistenceService.addDependenciesToResource(
            id,
            dependencies,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Get all the dependency files for a given application.
     *
     * @param id The id of the application to get the dependency files for. Not
     *           NULL/empty/blank.
     * @return The set of dependency files.
     * @throws NotFoundException If no application with the given ID exists
     */
    @GetMapping(value = "/{id}/dependencies", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getDependenciesForApplication(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        return this.persistenceService.getDependenciesForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Update the dependency files for a given application.
     *
     * @param id           The id of the application to update the dependency files for. Not
     *                     null/empty/blank.
     * @param dependencies The dependency files to replace existing dependency files with. Not
     *                     null/empty/blank.
     * @throws NotFoundException If no application with the given ID exists
     */
    @PutMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateDependenciesForApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws NotFoundException {
        log.info("Called with id {} and dependencies {}", id, dependencies);
        this.persistenceService.updateDependenciesForResource(
            id,
            dependencies,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Delete the all dependency files from a given application.
     *
     * @param id The id of the application to delete the dependency files from. Not
     *           null/empty/blank.
     * @throws NotFoundException If no application with the given ID exists
     */
    @DeleteMapping(value = "/{id}/dependencies")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllDependenciesForApplication(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        this.persistenceService.removeAllDependenciesForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Add new tags to a given application.
     *
     * @param id   The id of the application to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @throws NotFoundException If no application with the given ID exists
     */
    @PostMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addTagsForApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws NotFoundException {
        log.info("Called with id {} and config {}", id, tags);
        this.persistenceService.addTagsToResource(
            id,
            tags,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Get all the tags for a given application.
     *
     * @param id The id of the application to get the tags for. Not
     *           NULL/empty/blank.
     * @return The active set of tags.
     * @throws NotFoundException If no application with the given ID exists
     */
    @GetMapping(value = "/{id}/tags", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getTagsForApplication(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        // This is done so that the v3 tags (genie.id, genie.name) are added properly
        return DtoConverters.toV3Application(this.persistenceService.getApplication(id)).getTags();
    }

    /**
     * Update the tags for a given application.
     *
     * @param id   The id of the application to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing configuration
     *             files with. Not null/empty/blank.
     * @throws NotFoundException If no application with the given ID exists
     */
    @PutMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateTagsForApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws NotFoundException {
        log.info("Called with id {} and tags {}", id, tags);
        this.persistenceService.updateTagsForResource(
            id,
            tags,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Delete the all tags from a given application.
     *
     * @param id The id of the application to delete the tags from.
     *           Not null/empty/blank.
     * @throws NotFoundException If no application with the given ID exists
     */
    @DeleteMapping(value = "/{id}/tags")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllTagsForApplication(@PathVariable("id") final String id) throws NotFoundException {
        log.info("Called with id {}", id);
        this.persistenceService.removeAllTagsForResource(
            id,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Remove an tag from a given application.
     *
     * @param id  The id of the application to delete the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @throws NotFoundException If no application with the given ID exists
     */
    @DeleteMapping(value = "/{id}/tags/{tag}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeTagForApplication(
        @PathVariable("id") final String id,
        @PathVariable("tag") final String tag
    ) throws NotFoundException {
        log.info("Called with id {} and tag {}", id, tag);
        this.persistenceService.removeTagForResource(
            id,
            tag,
            com.netflix.genie.common.external.dtos.v4.Application.class
        );
    }

    /**
     * Get all the commands this application is associated with.
     *
     * @param id       The id of the application to get the commands for. Not
     *                 NULL/empty/blank.
     * @param statuses The various statuses of the commands to retrieve
     * @return The set of commands.
     * @throws NotFoundException          If no application with the given ID exists
     * @throws GeniePreconditionException When the statuses can't be parsed successfully
     */
    @GetMapping(value = "/{id}/commands", produces = MediaTypes.HAL_JSON_VALUE)
    public Set<EntityModel<Command>> getCommandsForApplication(
        @PathVariable("id") final String id,
        @RequestParam(value = "status", required = false) @Nullable final Set<String> statuses
    ) throws NotFoundException, GeniePreconditionException {
        log.info("Called with id {} and statuses {}", id, statuses);

        Set<CommandStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(CommandStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(
                    DtoConverters.toV4CommandStatus(com.netflix.genie.common.dto.CommandStatus.parse(status))
                );
            }
        }

        return this.persistenceService.getCommandsForApplication(id, enumStatuses)
            .stream()
            .map(DtoConverters::toV3Command)
            .map(this.commandModelAssembler::toModel)
            .collect(Collectors.toSet());
    }
}
