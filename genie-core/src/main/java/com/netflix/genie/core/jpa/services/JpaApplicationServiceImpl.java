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
package com.netflix.genie.core.jpa.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jpa.entities.ApplicationEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.specifications.JpaApplicationSpecs;
import com.netflix.genie.core.jpa.specifications.JpaCommandSpecs;
import com.netflix.genie.core.services.ApplicationService;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * JPA based implementation of the ApplicationService.
 *
 * @author amsharma
 * @author tgianos
 * @since 2.0.0
 */
@Transactional(
    rollbackFor = {
        GenieException.class,
        ConstraintViolationException.class
    }
)
@Slf4j
public class JpaApplicationServiceImpl implements ApplicationService {

    private final ObjectMapper mapper = new ObjectMapper();
    private final JpaApplicationRepository applicationRepo;
    private final JpaCommandRepository commandRepo;

    /**
     * Default constructor.
     *
     * @param applicationRepo The application repository to use
     * @param commandRepo     The command repository to use
     */
    public JpaApplicationServiceImpl(
        final JpaApplicationRepository applicationRepo,
        final JpaCommandRepository commandRepo
    ) {
        this.applicationRepo = applicationRepo;
        this.commandRepo = commandRepo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createApplication(
        @NotNull(message = "No application entered to create.")
        @Valid
        final Application app
    ) throws GenieException {
        log.debug("Called with application: {}", app.toString());
        final Optional<String> appId = app.getId();
        if (appId.isPresent() && this.applicationRepo.exists(appId.get())) {
            throw new GenieConflictException("An application with id " + appId.get() + " already exists");
        }

        final ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setId(app.getId().orElse(UUID.randomUUID().toString()));
        this.updateAndSaveApplicationEntity(applicationEntity, app);
        return applicationEntity.getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Application getApplication(
        @NotBlank(message = "No id entered. Unable to get")
        final String id
    ) throws GenieException {
        log.debug("Called with id {}", id);
        return this.findApplication(id).getDTO();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Application> getApplications(
        final String name,
        final String user,
        final Set<ApplicationStatus> statuses,
        final Set<String> tags,
        final String type,
        final Pageable page
    ) {
        log.debug("Called");

        @SuppressWarnings("unchecked")
        final Page<ApplicationEntity> applicationEntities
            = this.applicationRepo.findAll(JpaApplicationSpecs.find(name, user, statuses, tags, type), page);

        return applicationEntities.map(ApplicationEntity::getDTO);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateApplication(
        @NotBlank(message = "No application id entered. Unable to update.")
        final String id,
        @NotNull(message = "No application information entered. Unable to update.")
        @Valid
        final Application updateApp
    ) throws GenieException {
        if (!this.applicationRepo.exists(id)) {
            throw new GenieNotFoundException("No application information entered. Unable to update.");
        }
        final Optional<String> updateId = updateApp.getId();
        if (updateId.isPresent() && !id.equals(updateId.get())) {
            throw new GenieBadRequestException("Application id inconsistent with id passed in.");
        }

        log.debug("Called with app {}", updateApp.toString());
        this.updateAndSaveApplicationEntity(this.findApplication(id), updateApp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void patchApplication(@NotBlank final String id, @NotNull final JsonPatch patch) throws GenieException {
        final ApplicationEntity applicationEntity = this.findApplication(id);
        try {
            final Application appToPatch = applicationEntity.getDTO();
            log.debug("Will patch application {}. Original state: {}", id, appToPatch);
            final JsonNode applicationNode = this.mapper.readTree(appToPatch.toString());
            final JsonNode postPatchNode = patch.apply(applicationNode);
            final Application patchedApp = this.mapper.treeToValue(postPatchNode, Application.class);
            log.debug("Finished patching application {}. New state: {}", id, patchedApp);
            this.updateAndSaveApplicationEntity(applicationEntity, patchedApp);
        } catch (final JsonPatchException | IOException e) {
            log.error("Unable to patch application {} with patch {} due to exception.", id, patch, e);
            throw new GenieServerException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllApplications() throws GenieException {
        log.debug("Called");
        // Check to make sure the application isn't tied to any existing commands
        for (final ApplicationEntity applicationEntity : this.applicationRepo.findAll()) {
            final Set<CommandEntity> commandEntities = applicationEntity.getCommands();
            if (commandEntities != null && !commandEntities.isEmpty()) {
                throw new GeniePreconditionException(
                    "Unable to delete app " + applicationEntity.getId() + " as it is attached to "
                        + commandEntities.size() + " commands still."
                );
            }
        }
        this.applicationRepo.deleteAll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteApplication(
        @NotBlank(message = "No application id entered. Unable to delete.")
        final String id
    ) throws GenieException {
        log.debug("Called with id {}", id);
        final ApplicationEntity applicationEntity = this.findApplication(id);
        final Set<CommandEntity> commandEntities = applicationEntity.getCommands();
        if (commandEntities != null && !commandEntities.isEmpty()) {
            throw new GeniePreconditionException(
                "Unable to delete app " + applicationEntity.getId() + " as it is attached to "
                    + commandEntities.size() + " commands."
            );
        }

        this.applicationRepo.delete(applicationEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addConfigsToApplication(
        @NotBlank(message = "No application id entered. Unable to add configurations.")
        final String id,
        @NotEmpty(message = "No configuration files entered.")
        final Set<String> configs
    ) throws GenieException {
        this.findApplication(id).getConfigs().addAll(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to get configs.")
        final String id
    ) throws GenieException {
        return this.findApplication(id).getConfigs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to update configurations.")
        final String id,
        @NotNull(message = "No configs entered. Unable to update. If you want, use delete API.")
        final Set<String> configs
    ) throws GenieException {
        this.findApplication(id).setConfigs(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to remove configs.")
        final String id
    ) throws GenieException {
        this.findApplication(id).getConfigs().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeConfigForApplication(
        @NotBlank(message = "No application id entered. Unable to remove configuration.")
        final String id,
        @NotBlank(message = "No config entered. Unable to remove.")
        final String config
    ) throws GenieException {
        this.findApplication(id).getConfigs().remove(config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to add dependencies.")
        final String id,
        @NotEmpty(message = "No dependencies entered. Unable to add dependencies.")
        final Set<String> dependencies
    ) throws GenieException {
        this.findApplication(id).getDependencies().addAll(dependencies);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to get dependencies.")
        final String id
    ) throws GenieException {
        return this.findApplication(id).getDependencies();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to update dependencies.")
        final String id,
        @NotNull(message = "No dependencies entered. Unable to update.")
        final Set<String> dependencies
    ) throws GenieException {
        this.findApplication(id).setDependencies(dependencies);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to remove dependencies.")
        final String id
    ) throws GenieException {
        this.findApplication(id).getDependencies().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDependencyForApplication(
        @NotBlank(message = "No application id entered. Unable to remove dependency.")
        final String id,
        @NotBlank(message = "No dependency entered. Unable to remove dependency.")
        final String dependency
    ) throws GenieException {
        this.findApplication(id).getDependencies().remove(dependency);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to add tags.")
        final String id,
        @NotEmpty(message = "No tags entered. Unable to add.")
        final Set<String> tags
    ) throws GenieException {
        final ApplicationEntity app = this.findApplication(id);
        final Set<String> appTags = app.getTags();
        appTags.addAll(tags);
        app.setTags(appTags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForApplication(
        @NotBlank(message = "No application id entered. Cannot retrieve tags.")
        final String id
    ) throws GenieException {
        return this.findApplication(id).getTags();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to update tags.")
        final String id,
        @NotNull(message = "No tags entered unable to update tags.")
        final Set<String> tags
    ) throws GenieException {
        this.findApplication(id).setTags(tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to remove tags.")
        final String id
    ) throws GenieException {
        this.findApplication(id).setTags(Sets.newHashSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeTagForApplication(
        @NotBlank(message = "No application id entered. Unable to remove tag.")
        final String id,
        @NotBlank(message = "No tag entered. Unable to remove.")
        final String tag
    ) throws GenieException {
        final ApplicationEntity app = this.findApplication(id);
        final Set<String> tags = app.getTags();
        tags.remove(tag);
        app.setTags(tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<Command> getCommandsForApplication(
        @NotBlank(message = "No application id entered. Unable to get commands.")
        final String id,
        final Set<CommandStatus> statuses
    ) throws GenieException {
        if (!this.applicationRepo.exists(id)) {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
        @SuppressWarnings("unchecked")
        final List<CommandEntity> commandEntities = this.commandRepo.findAll(
            JpaCommandSpecs.findCommandsForApplication(id, statuses)
        );
        return commandEntities
            .stream()
            .map(CommandEntity::getDTO)
            .collect(Collectors.toSet());
    }

    /**
     * Helper to find an application entity based on ID.
     *
     * @param id The id of the application to find
     * @return The application entity if one is found
     * @throws GenieNotFoundException If no application is found
     */
    private ApplicationEntity findApplication(final String id) throws GenieNotFoundException {
        final ApplicationEntity applicationEntity = this.applicationRepo.findOne(id);
        if (applicationEntity != null) {
            return applicationEntity;
        } else {
            throw new GenieNotFoundException("No application with id " + id);
        }
    }

    private void updateAndSaveApplicationEntity(final ApplicationEntity entity, final Application dto) {
        entity.setName(dto.getName());
        entity.setUser(dto.getUser());
        entity.setVersion(dto.getVersion());
        final Optional<String> description = dto.getDescription();
        entity.setDescription(description.isPresent() ? description.get() : null);
        entity.setStatus(dto.getStatus());
        final Optional<String> setupFile = dto.getSetupFile();
        entity.setSetupFile(setupFile.isPresent() ? setupFile.get() : null);
        entity.setConfigs(dto.getConfigs());
        entity.setDependencies(dto.getDependencies());
        entity.setTags(dto.getTags());
        final Optional<String> type = dto.getType();
        entity.setType(type.isPresent() ? type.get() : null);

        this.applicationRepo.save(entity);
    }
}
