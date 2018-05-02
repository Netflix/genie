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
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.hateoas.assemblers.ApplicationResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.CommandResourceAssembler;
import com.netflix.genie.web.hateoas.resources.ApplicationResource;
import com.netflix.genie.web.hateoas.resources.CommandResource;
import com.netflix.genie.web.services.ApplicationPersistenceService;
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

    private final ApplicationPersistenceService applicationPersistenceService;
    private final ApplicationResourceAssembler applicationResourceAssembler;
    private final CommandResourceAssembler commandResourceAssembler;

    /**
     * Constructor.
     *
     * @param applicationPersistenceService The application configuration service to use.
     * @param applicationResourceAssembler  The assembler used to create Application resources.
     * @param commandResourceAssembler      The assembler used to create Command resources.
     */
    @Autowired
    public ApplicationRestController(
        final ApplicationPersistenceService applicationPersistenceService,
        final ApplicationResourceAssembler applicationResourceAssembler,
        final CommandResourceAssembler commandResourceAssembler
    ) {
        this.applicationPersistenceService = applicationPersistenceService;
        this.applicationResourceAssembler = applicationResourceAssembler;
        this.commandResourceAssembler = commandResourceAssembler;
    }

    /**
     * Create an Application.
     *
     * @param app The application to create
     * @return The created application configuration
     * @throws GenieException For any error
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<Void> createApplication(@RequestBody final Application app) throws GenieException {
        log.debug("Called to create new application");
        final String id = this.applicationPersistenceService.createApplication(
            DtoConverters.toV4ApplicationRequest(app)
        );
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
     * @throws GenieException For any error
     */
    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteAllApplications() throws GenieException {
        log.debug("Delete all Applications");
        this.applicationPersistenceService.deleteAllApplications();
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
    public PagedResources<ApplicationResource> getApplications(
        @RequestParam(value = "name", required = false) final String name,
        @RequestParam(value = "user", required = false) final String user,
        @RequestParam(value = "status", required = false) final Set<String> statuses,
        @RequestParam(value = "tag", required = false) final Set<String> tags,
        @RequestParam(value = "type", required = false) final String type,
        @PageableDefault(sort = {"updated"}, direction = Sort.Direction.DESC) final Pageable page,
        final PagedResourcesAssembler<Application> assembler
    ) throws GenieException {
        log.debug("Called [name | user | status | tags | type | pageable]");
        log.debug("{} | {} | {} | {} | | {} | {}", name, user, statuses, tags, type, page);

        Set<ApplicationStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(ApplicationStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(ApplicationStatus.parse(status));
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
                                DtoConverters.toV3Application(this.applicationPersistenceService.getApplication(id))
                            );
                        } catch (final GenieException ge) {
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

                applications = this.applicationPersistenceService
                    .getApplications(finalName.orElse(null), user, enumStatuses, finalTags, type, page)
                    .map(DtoConverters::toV3Application);
            } else {
                applications = this.applicationPersistenceService
                    .getApplications(name, user, enumStatuses, finalTags, type, page)
                    .map(DtoConverters::toV3Application);
            }
        } else {
            applications = this.applicationPersistenceService
                .getApplications(name, user, enumStatuses, tags, type, page)
                .map(DtoConverters::toV3Application);
        }

        final Link self = ControllerLinkBuilder.linkTo(
            ControllerLinkBuilder
                .methodOn(ApplicationRestController.class)
                .getApplications(name, user, statuses, tags, type, page, assembler)
        ).withSelfRel();

        return assembler.toResource(
            applications,
            this.applicationResourceAssembler,
            self
        );
    }

    /**
     * Get Application for given id.
     *
     * @param id unique id for application configuration
     * @return The application configuration
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ApplicationResource getApplication(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called to get Application for id {}", id);
        return this.applicationResourceAssembler.toResource(
            DtoConverters.toV3Application(this.applicationPersistenceService.getApplication(id))
        );
    }

    /**
     * Update application.
     *
     * @param id        unique id for configuration to update
     * @param updateApp contains the application information to update
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateApplication(
        @PathVariable("id") final String id,
        @RequestBody final Application updateApp
    ) throws GenieException {
        log.debug("called to update application {} with info {}", id, updateApp);
        this.applicationPersistenceService.updateApplication(id, DtoConverters.toV4Application(updateApp));
    }

    /**
     * Patch an application using JSON Patch.
     *
     * @param id    The id of the application to patch
     * @param patch The JSON Patch instructions
     * @throws GenieException On error
     */
    @PatchMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void patchApplication(
        @PathVariable("id") final String id,
        @RequestBody final JsonPatch patch
    ) throws GenieException {
        log.debug("Called to patch application {} with patch {}", id, patch);
        final Application currentApp = DtoConverters.toV3Application(
            this.applicationPersistenceService.getApplication(id)
        );

        try {
            log.debug("Will patch application {}. Original state: {}", id, currentApp);
            final JsonNode applicationNode = GenieObjectMapper.getMapper().valueToTree(currentApp);
            final JsonNode postPatchNode = patch.apply(applicationNode);
            final Application patchedApp = GenieObjectMapper.getMapper().treeToValue(postPatchNode, Application.class);
            log.debug("Finished patching application {}. New state: {}", id, patchedApp);
            this.applicationPersistenceService.updateApplication(id, DtoConverters.toV4Application(patchedApp));
        } catch (final JsonPatchException | IOException e) {
            log.error("Unable to patch application {} with patch {} due to exception.", id, patch, e);
            throw new GenieServerException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Delete an application configuration from database.
     *
     * @param id unique id of configuration to delete
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteApplication(@PathVariable("id") final String id) throws GenieException {
        log.debug("Delete an application with id {}", id);
        this.applicationPersistenceService.deleteApplication(id);
    }

    /**
     * Add new configuration files to a given application.
     *
     * @param id      The id of the application to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addConfigsToApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws GenieException {
        log.debug("Called with id {} and config {}", id, configs);
        this.applicationPersistenceService.addConfigsToApplication(id, configs);
    }

    /**
     * Get all the configuration files for a given application.
     *
     * @param id The id of the application to get the configuration files for.
     *           Not NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/configs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getConfigsForApplication(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return this.applicationPersistenceService.getConfigsForApplication(id);
    }

    /**
     * Update the configuration files for a given application.
     *
     * @param id      The id of the application to update the configuration files
     *                for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     *                files with. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}/configs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateConfigsForApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> configs
    ) throws GenieException {
        log.debug("Called with id {} and configs {}", id, configs);
        this.applicationPersistenceService.updateConfigsForApplication(id, configs);
    }

    /**
     * Delete the all configuration files from a given application.
     *
     * @param id The id of the application to delete the configuration files
     *           from. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/configs")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllConfigsForApplication(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        this.applicationPersistenceService.removeAllConfigsForApplication(id);
    }

    /**
     * Add new dependency files for a given application.
     *
     * @param id           The id of the application to add the dependency file to. Not
     *                     null/empty/blank.
     * @param dependencies The dependency files to add. Not null.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addDependenciesForApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws GenieException {
        log.debug("Called with id {} and dependencies {}", id, dependencies);
        this.applicationPersistenceService.addDependenciesForApplication(id, dependencies);
    }

    /**
     * Get all the dependency files for a given application.
     *
     * @param id The id of the application to get the dependency files for. Not
     *           NULL/empty/blank.
     * @return The set of dependency files.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/dependencies", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getDependenciesForApplication(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return this.applicationPersistenceService.getDependenciesForApplication(id);
    }

    /**
     * Update the dependency files for a given application.
     *
     * @param id           The id of the application to update the dependency files for. Not
     *                     null/empty/blank.
     * @param dependencies The dependency files to replace existing dependency files with. Not
     *                     null/empty/blank.
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}/dependencies", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateDependenciesForApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> dependencies
    ) throws GenieException {
        log.debug("Called with id {} and dependencies {}", id, dependencies);
        this.applicationPersistenceService.updateDependenciesForApplication(id, dependencies);
    }

    /**
     * Delete the all dependency files from a given application.
     *
     * @param id The id of the application to delete the dependency files from. Not
     *           null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/dependencies")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllDependenciesForApplication(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        this.applicationPersistenceService.removeAllDependenciesForApplication(id);
    }

    /**
     * Add new tags to a given application.
     *
     * @param id   The id of the application to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @PostMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addTagsForApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws GenieException {
        log.debug("Called with id {} and config {}", id, tags);
        this.applicationPersistenceService.addTagsForApplication(id, tags);
    }

    /**
     * Get all the tags for a given application.
     *
     * @param id The id of the application to get the tags for. Not
     *           NULL/empty/blank.
     * @return The active set of tags.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/tags", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public Set<String> getTagsForApplication(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return DtoConverters.toV3Application(this.applicationPersistenceService.getApplication(id)).getTags();
    }

    /**
     * Update the tags for a given application.
     *
     * @param id   The id of the application to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing configuration
     *             files with. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @PutMapping(value = "/{id}/tags", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateTagsForApplication(
        @PathVariable("id") final String id,
        @RequestBody final Set<String> tags
    ) throws GenieException {
        log.debug("Called with id {} and tags {}", id, tags);
        this.applicationPersistenceService.updateTagsForApplication(id, tags);
    }

    /**
     * Delete the all tags from a given application.
     *
     * @param id The id of the application to delete the tags from.
     *           Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/tags")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeAllTagsForApplication(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called with id {}", id);
        this.applicationPersistenceService.removeAllTagsForApplication(id);
    }

    /**
     * Remove an tag from a given application.
     *
     * @param id  The id of the application to delete the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @DeleteMapping(value = "/{id}/tags/{tag}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeTagForApplication(
        @PathVariable("id") final String id,
        @PathVariable("tag") final String tag
    ) throws GenieException {
        log.debug("Called with id {} and tag {}", id, tag);
        this.applicationPersistenceService.removeTagForApplication(id, tag);
    }

    /**
     * Get all the commands this application is associated with.
     *
     * @param id       The id of the application to get the commands for. Not
     *                 NULL/empty/blank.
     * @param statuses The various statuses of the commands to retrieve
     * @return The set of commands.
     * @throws GenieException For any error
     */
    @GetMapping(value = "/{id}/commands", produces = MediaTypes.HAL_JSON_VALUE)
    public Set<CommandResource> getCommandsForApplication(
        @PathVariable("id") final String id,
        @RequestParam(value = "status", required = false) final Set<String> statuses
    ) throws GenieException {
        log.debug("Called with id {}", id);

        Set<CommandStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = EnumSet.noneOf(CommandStatus.class);
            for (final String status : statuses) {
                enumStatuses.add(CommandStatus.parse(status));
            }
        }

        return this.applicationPersistenceService.getCommandsForApplication(id, enumStatuses)
            .stream()
            .map(DtoConverters::toV3Command)
            .map(this.commandResourceAssembler::toResource)
            .collect(Collectors.toSet());
    }
}
