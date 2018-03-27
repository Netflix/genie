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
package com.netflix.genie.web.jpa.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.v4.Application;
import com.netflix.genie.common.dto.v4.ApplicationMetadata;
import com.netflix.genie.common.dto.v4.ApplicationRequest;
import com.netflix.genie.common.dto.v4.Command;
import com.netflix.genie.common.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.entities.v4.EntityDtoConverters;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.jpa.specifications.JpaApplicationSpecs;
import com.netflix.genie.web.jpa.specifications.JpaCommandSpecs;
import com.netflix.genie.web.services.ApplicationService;
import com.netflix.genie.web.services.FileService;
import com.netflix.genie.web.services.TagService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
public class JpaApplicationServiceImpl extends JpaBaseService implements ApplicationService {

    private final JpaApplicationRepository applicationRepository;
    private final JpaCommandRepository commandRepository;

    /**
     * Default constructor.
     *
     * @param tagService            The tag service to use
     * @param tagRepository         The tag repository to use
     * @param fileService           The file service to use
     * @param fileRepository        The file repository to use
     * @param applicationRepository The application repository to use
     * @param commandRepository     The command repository to use
     */
    public JpaApplicationServiceImpl(
        final TagService tagService,
        final JpaTagRepository tagRepository,
        final FileService fileService,
        final JpaFileRepository fileRepository,
        final JpaApplicationRepository applicationRepository,
        final JpaCommandRepository commandRepository
    ) {
        super(tagService, tagRepository, fileService, fileRepository);
        this.applicationRepository = applicationRepository;
        this.commandRepository = commandRepository;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createApplication(
        @NotNull(message = "No application entered to create.")
        @Valid final ApplicationRequest applicationRequest
    ) throws GenieException {
        log.debug("Called to create new application with request metadata {}", applicationRequest);
        final ApplicationEntity applicationEntity = this.createApplicationEntity(applicationRequest);
        try {
            this.applicationRepository.save(applicationEntity);
        } catch (final DataIntegrityViolationException e) {
            throw new GenieConflictException(
                "An application with id " + applicationEntity.getUniqueId() + " already exists",
                e
            );
        }
        return applicationEntity.getUniqueId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Application getApplication(
        @NotBlank(message = "No id entered. Unable to get") final String id
    ) throws GenieException {
        log.debug("Called with id {}", id);
        return EntityDtoConverters.toV4ApplicationDto(this.findApplication(id));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Application> getApplications(
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<ApplicationStatus> statuses,
        @Nullable final Set<String> tags,
        @Nullable final String type,
        final Pageable page
    ) {
        log.debug("Called");

        final Set<TagEntity> tagEntities;
        // Find the tag entity references. If one doesn't exist return empty page as if the tag doesn't exist
        // no entities tied to that tag will exist either and today our search for tags is an AND
        if (tags != null) {
            tagEntities = this.getTagRepository().findByTagIn(tags);
            if (tagEntities.size() != tags.size()) {
                return new PageImpl<>(new ArrayList<Application>(), page, 0);
            }
        } else {
            tagEntities = null;
        }

        @SuppressWarnings("unchecked") final Page<ApplicationEntity> applicationEntities = this.applicationRepository
            .findAll(
                JpaApplicationSpecs.find(name, user, statuses, tagEntities, type),
                page
            );

        return applicationEntities.map(EntityDtoConverters::toV4ApplicationDto);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateApplication(
        @NotBlank(message = "No application id entered. Unable to update.") final String id,
        @NotNull(message = "No application information entered. Unable to update.")
        @Valid final Application updateApp
    ) throws GenieException {
        if (!this.applicationRepository.existsByUniqueId(id)) {
            throw new GenieNotFoundException("No application with id " + id + " exists. Unable to update.");
        }
        final String updateId = updateApp.getId();
        if (!id.equals(updateId)) {
            throw new GenieBadRequestException("Application id inconsistent with id passed in.");
        }

        log.debug("Called with app {}", updateApp);
        this.updateEntityWithDtoContents(this.findApplication(id), updateApp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void patchApplication(@NotBlank final String id, @NotNull final JsonPatch patch) throws GenieException {
        final ApplicationEntity applicationEntity = this.findApplication(id);
        try {
            final Application appToPatch = EntityDtoConverters.toV4ApplicationDto(applicationEntity);
            log.debug("Will patch application {}. Original state: {}", id, appToPatch);
            final JsonNode applicationNode = GenieObjectMapper.getMapper().valueToTree(appToPatch);
            final JsonNode postPatchNode = patch.apply(applicationNode);
            final Application patchedApp = GenieObjectMapper.getMapper().treeToValue(postPatchNode, Application.class);
            log.debug("Finished patching application {}. New state: {}", id, patchedApp);
            this.updateEntityWithDtoContents(applicationEntity, patchedApp);
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
        for (final ApplicationEntity applicationEntity : this.applicationRepository.findAll()) {
            this.checkCommands(applicationEntity);
        }
        this.applicationRepository.deleteAll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteApplication(
        @NotBlank(message = "No application id entered. Unable to delete.") final String id
    ) throws GenieException {
        log.debug("Called with id {}", id);
        final ApplicationEntity applicationEntity = this.findApplication(id);
        this.checkCommands(applicationEntity);
        this.applicationRepository.delete(applicationEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addConfigsToApplication(
        @NotBlank(message = "No application id entered. Unable to add configurations.") final String id,
        @NotEmpty(message = "No configuration files entered.") final Set<String> configs
    ) throws GenieException {
        this.findApplication(id).getConfigs().addAll(this.createAndGetFileEntities(configs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to get configs.") final String id
    ) throws GenieException {
        return this.findApplication(id).getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to update configurations.") final String id,
        @NotNull(
            message = "No configs entered. Unable to update. If you want, use delete API."
        ) final Set<String> configs
    ) throws GenieException {
        this.findApplication(id).setConfigs(this.createAndGetFileEntities(configs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to remove configs.") final String id
    ) throws GenieException {
        this.findApplication(id).getConfigs().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeConfigForApplication(
        @NotBlank(message = "No application id entered. Unable to remove configuration.") final String id,
        @NotBlank(message = "No config entered. Unable to remove.") final String config
    ) throws GenieException {
        this.getFileRepository().findByFile(config).ifPresent(this.findApplication(id).getConfigs()::remove);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to add dependencies.") final String id,
        @NotEmpty(message = "No dependencies entered. Unable to add dependencies.") final Set<String> dependencies
    ) throws GenieException {
        this.findApplication(id).getDependencies().addAll(this.createAndGetFileEntities(dependencies));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to get dependencies.") final String id
    ) throws GenieException {
        return this.findApplication(id).getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to update dependencies.") final String id,
        @NotNull(message = "No dependencies entered. Unable to update.") final Set<String> dependencies
    ) throws GenieException {
        this.findApplication(id).setDependencies(this.createAndGetFileEntities(dependencies));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to remove dependencies.") final String id
    ) throws GenieException {
        this.findApplication(id).getDependencies().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDependencyForApplication(
        @NotBlank(message = "No application id entered. Unable to remove dependency.") final String id,
        @NotBlank(message = "No dependency entered. Unable to remove dependency.") final String dependency
    ) throws GenieException {
        this.getFileRepository().findByFile(dependency).ifPresent(this.findApplication(id).getDependencies()::remove);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to add tags.") final String id,
        @NotEmpty(message = "No tags entered. Unable to add.") final Set<String> tags
    ) throws GenieException {
        this.findApplication(id).getTags().addAll(this.createAndGetTagEntities(tags));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForApplication(
        @NotBlank(message = "No application id entered. Cannot retrieve tags.") final String id
    ) throws GenieException {
        return this.findApplication(id).getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to update tags.") final String id,
        @NotNull(message = "No tags entered unable to update tags.") final Set<String> tags
    ) throws GenieException {
        final ApplicationEntity applicationEntity = this.findApplication(id);
        final Set<TagEntity> newTags = this.createAndGetTagEntities(tags);
        applicationEntity.setTags(newTags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to remove tags.") final String id
    ) throws GenieException {
        final Set<TagEntity> tags = this.findApplication(id).getTags();
        // Remove all the tags except the ones that start with "genie."
        tags.removeAll(
            tags
                .stream()
                .filter(
                    tagEntity -> !tagEntity.getTag().startsWith(JpaBaseService.GENIE_TAG_NAMESPACE)
                )
                .collect(Collectors.toSet())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeTagForApplication(
        @NotBlank(message = "No application id entered. Unable to remove tag.") final String id,
        @NotBlank(message = "No tag entered. Unable to remove.") final String tag
    ) throws GenieException {
        if (!tag.startsWith(JpaBaseService.GENIE_TAG_NAMESPACE)) {
            this.getTagRepository().findByTag(tag).ifPresent(this.findApplication(id).getTags()::remove);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<Command> getCommandsForApplication(
        @NotBlank(message = "No application id entered. Unable to get commands.") final String id,
        @Nullable final Set<CommandStatus> statuses
    ) throws GenieException {
        if (!this.applicationRepository.existsByUniqueId(id)) {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
        @SuppressWarnings("unchecked") final List<CommandEntity> commandEntities = this.commandRepository.findAll(
            JpaCommandSpecs.findCommandsForApplication(id, statuses)
        );
        return commandEntities
            .stream()
            .map(EntityDtoConverters::toV4CommandDto)
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
        return this.applicationRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new GenieNotFoundException("No application with id " + id));
    }

    private ApplicationEntity createApplicationEntity(final ApplicationRequest request) throws GenieException {
        final ExecutionEnvironment resources = request.getResources();
        final ApplicationMetadata metadata = request.getMetadata();

        final ApplicationEntity entity = new ApplicationEntity();
        entity.setUniqueId(request.getRequestedId().orElse(UUID.randomUUID().toString()));
        this.setEntityResources(entity, resources);
        this.setEntityTags(entity, metadata);
        this.setEntityApplicationMetadata(entity, metadata);

        return entity;
    }

    private void updateEntityWithDtoContents(
        final ApplicationEntity entity,
        final Application dto
    ) throws GenieException {
        final ExecutionEnvironment resources = dto.getResources();
        final ApplicationMetadata metadata = dto.getMetadata();

        // Save all the unowned entities first to avoid unintended flushes
        this.setEntityResources(entity, resources);
        this.setEntityTags(entity, metadata);
        this.setEntityApplicationMetadata(entity, metadata);
    }

    private void setEntityApplicationMetadata(final ApplicationEntity entity, final ApplicationMetadata metadata) {
        // NOTE: These are all called in case someone has changed it to set something to null. DO NOT use ifPresent
        entity.setName(metadata.getName());
        entity.setUser(metadata.getUser());
        entity.setVersion(metadata.getVersion().orElse(null));
        entity.setDescription(metadata.getDescription().orElse(null));
        entity.setStatus(metadata.getStatus());
        entity.setType(metadata.getType().orElse(null));
        EntityDtoConverters.setEntityMetadata(GenieObjectMapper.getMapper(), metadata, entity);
    }

    private void setEntityResources(
        final ApplicationEntity entity,
        final ExecutionEnvironment resources
    ) throws GenieException {
        // Save all the unowned entities first to avoid unintended flushes
        final Set<FileEntity> configs = this.createAndGetFileEntities(resources.getConfigs());
        final Set<FileEntity> dependencies = this.createAndGetFileEntities(resources.getDependencies());
        final FileEntity setupFile = resources.getSetupFile().isPresent()
            ? this.createAndGetFileEntity(resources.getSetupFile().get())
            : null;

        entity.setConfigs(configs);
        entity.setDependencies(dependencies);
        entity.setSetupFile(setupFile);
    }

    private void setEntityTags(
        final ApplicationEntity entity,
        final ApplicationMetadata metadata
    ) throws GenieException {
        entity.setTags(this.createAndGetTagEntities(metadata.getTags()));
    }

    private void checkCommands(final ApplicationEntity applicationEntity) throws GeniePreconditionException {
        final Set<CommandEntity> commands = applicationEntity.getCommands();
        if (commands != null && !commands.isEmpty()) {
            throw new GeniePreconditionException(
                "Unable to delete app "
                    + applicationEntity.getId()
                    + " as it is attached to "
                    + commands.size()
                    + " commands still."
            );
        }
    }
}
