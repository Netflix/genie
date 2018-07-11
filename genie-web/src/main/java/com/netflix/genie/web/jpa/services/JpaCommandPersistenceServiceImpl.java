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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.CommandMetadata;
import com.netflix.genie.common.internal.dto.v4.CommandRequest;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.ClusterEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.entities.v4.EntityDtoConverters;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.specifications.JpaClusterSpecs;
import com.netflix.genie.web.jpa.specifications.JpaCommandSpecs;
import com.netflix.genie.web.services.CommandPersistenceService;
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
import java.util.stream.Collectors;

/**
 * Implementation of the CommandPersistenceService interface.
 *
 * @author amsharma
 * @author tgianos
 * @since 2.0.0
 */
@Transactional(
    rollbackFor = {
        GenieException.class,
        GenieRuntimeException.class,
        ConstraintViolationException.class
    }
)
@Slf4j
public class JpaCommandPersistenceServiceImpl extends JpaBaseService implements CommandPersistenceService {

    /**
     * Default constructor.
     *
     * @param tagPersistenceService  The {@link JpaTagPersistenceService} to use
     * @param filePersistenceService The {@link JpaFilePersistenceService} to use
     * @param applicationRepository  The {@link JpaApplicationRepository} to use
     * @param clusterRepository      The {@link JpaClusterRepository} to use
     * @param commandRepository      The {@link JpaCommandRepository} to use
     */
    public JpaCommandPersistenceServiceImpl(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        super(
            tagPersistenceService,
            filePersistenceService,
            applicationRepository,
            clusterRepository,
            commandRepository
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createCommand(
        @NotNull(message = "No command entered. Unable to create.")
        @Valid final CommandRequest request
    ) throws GenieException {
        log.debug("Called to create command {}", request);
        final CommandEntity commandEntity = this.createCommandEntity(request);
        try {
            this.getCommandRepository().save(commandEntity);
        } catch (final DataIntegrityViolationException e) {
            throw new GenieConflictException(
                "A command with id " + commandEntity.getUniqueId() + " already exists",
                e
            );
        }
        return commandEntity.getUniqueId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Command getCommand(
        @NotBlank(message = "No id entered unable to get.") final String id
    ) throws GenieException {
        log.debug("called");
        return EntityDtoConverters.toV4CommandDto(this.findCommand(id));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Command> getCommands(
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<CommandStatus> statuses,
        @Nullable final Set<String> tags,
        final Pageable page
    ) {
        log.debug("Called");

        final Set<TagEntity> tagEntities;
        // Find the tag entity references. If one doesn't exist return empty page as if the tag doesn't exist
        // no entities tied to that tag will exist either and today our search for tags is an AND
        if (tags != null) {
            tagEntities = this.getTagPersistenceService().getTags(tags);
            if (tagEntities.size() != tags.size()) {
                return new PageImpl<>(new ArrayList<>(), page, 0);
            }
        } else {
            tagEntities = null;
        }

        final Page<CommandEntity> commandEntities = this.getCommandRepository().findAll(
            JpaCommandSpecs.find(
                name,
                user,
                statuses,
                tagEntities
            ),
            page
        );

        return commandEntities.map(EntityDtoConverters::toV4CommandDto);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCommand(
        @NotBlank(message = "No id entered. Unable to update.") final String id,
        @NotNull(message = "No command information entered. Unable to update.")
        @Valid final Command updateCommand
    ) throws GenieException {
        if (!this.getCommandRepository().existsByUniqueId(id)) {
            throw new GenieNotFoundException("No command exists with the given id. Unable to update.");
        }
        final String updateId = updateCommand.getId();
        if (!id.equals(updateId)) {
            throw new GenieBadRequestException("Command id inconsistent with id passed in.");
        }

        log.debug("Called to update command with id {} {}", id, updateCommand);

        this.updateEntityWithDtoContents(this.findCommand(id), updateCommand);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void patchCommand(@NotBlank final String id, @NotNull final JsonPatch patch) throws GenieException {
        final CommandEntity commandEntity = this.findCommand(id);
        try {
            final Command commandToPatch = EntityDtoConverters.toV4CommandDto(commandEntity);
            log.debug("Will patch command {}. Original state: {}", id, commandToPatch);
            final JsonNode commandNode = GenieObjectMapper.getMapper().valueToTree(commandToPatch);
            final JsonNode postPatchNode = patch.apply(commandNode);
            final Command patchedCommand = GenieObjectMapper.getMapper().treeToValue(postPatchNode, Command.class);
            log.debug("Finished patching command {}. New state: {}", id, patchedCommand);
            this.updateEntityWithDtoContents(commandEntity, patchedCommand);
        } catch (final JsonPatchException | IOException e) {
            log.error("Unable to patch cluster {} with patch {} due to exception.", id, patch, e);
            throw new GenieServerException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllCommands() throws GenieException {
        log.debug("Called to delete all commands");
        for (final CommandEntity commandEntity : this.getCommandRepository().findAll()) {
            this.deleteCommand(commandEntity.getUniqueId());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteCommand(
        @NotBlank(message = "No id entered. Unable to delete.") final String id
    ) throws GenieException {
        log.debug("Called to delete command config with id {}", id);
        final CommandEntity commandEntity = this.findCommand(id);

        //Remove the command from the associated Application references
        final List<ApplicationEntity> originalApps = commandEntity.getApplications();
        if (originalApps != null) {
            final List<ApplicationEntity> applicationEntities = Lists.newArrayList(originalApps);
            applicationEntities.forEach(commandEntity::removeApplication);
        }
        //Remove the command from the associated cluster references
        final Set<ClusterEntity> originalClusters = commandEntity.getClusters();
        if (originalClusters != null) {
            final Set<ClusterEntity> clusterEntities = Sets.newHashSet(originalClusters);
            clusterEntities.forEach(clusterEntity -> clusterEntity.removeCommand(commandEntity));
        }
        this.getCommandRepository().delete(commandEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addConfigsForCommand(
        @NotBlank(message = "No command id entered. Unable to add configurations.") final String id,
        @NotEmpty(message = "No configuration files entered. Unable to add.") final Set<String> configs
    ) throws GenieException {
        this.findCommand(id).getConfigs().addAll(this.createAndGetFileEntities(configs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForCommand(
        @NotBlank(message = "No command id entered. Unable to get configs.") final String id
    ) throws GenieException {
        return this.findCommand(id).getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateConfigsForCommand(
        @NotBlank(message = "No command id entered. Unable to update configurations.") final String id,
        @NotEmpty(message = "No configs entered. Unable to update.") final Set<String> configs
    ) throws GenieException {
        this.findCommand(id).setConfigs(this.createAndGetFileEntities(configs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllConfigsForCommand(
        @NotBlank(message = "No command id entered. Unable to remove configs.") final String id
    ) throws GenieException {
        this.findCommand(id).getConfigs().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeConfigForCommand(
        @NotBlank(message = "No command id entered. Unable to remove configuration.") final String id,
        @NotBlank(message = "No config entered. Unable to remove.") final String config
    ) throws GenieException {
        this.getFilePersistenceService().getFile(config).ifPresent(this.findCommand(id).getConfigs()::remove);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addDependenciesForCommand(
        @NotBlank(message = "No command id entered. Unable to add dependencies.") final String id,
        @NotEmpty(message = "No dependencies entered. Unable to add dependencies.") final Set<String> dependencies
    ) throws GenieException {
        this.findCommand(id).getDependencies().addAll(this.createAndGetFileEntities(dependencies));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getDependenciesForCommand(
        @NotBlank(message = "No command id entered. Unable to get dependencies.") final String id
    ) throws GenieException {
        return this.findCommand(id).getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDependenciesForCommand(
        @NotBlank(message = "No command id entered. Unable to update dependencies.") final String id,
        @NotNull(message = "No dependencies entered. Unable to update.") final Set<String> dependencies
    ) throws GenieException {
        this.findCommand(id).setDependencies(this.createAndGetFileEntities(dependencies));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllDependenciesForCommand(
        @NotBlank(message = "No command id entered. Unable to remove dependencies.") final String id
    ) throws GenieException {
        this.findCommand(id).getDependencies().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDependencyForCommand(
        @NotBlank(message = "No command id entered. Unable to remove dependency.") final String id,
        @NotBlank(message = "No dependency entered. Unable to remove dependency.") final String dependency
    ) throws GenieException {
        this.getFilePersistenceService().getFile(dependency).ifPresent(this.findCommand(id).getDependencies()::remove);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addTagsForCommand(
        @NotBlank(message = "No command id entered. Unable to add tags.") final String id,
        @NotEmpty(message = "No tags entered. Unable to add.") final Set<String> tags
    ) throws GenieException {
        this.findCommand(id).getTags().addAll(this.createAndGetTagEntities(tags));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForCommand(
        @NotBlank(message = "No command id sent. Cannot retrieve tags.") final String id
    ) throws GenieException {
        return this.findCommand(id).getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTagsForCommand(
        @NotBlank(message = "No command id entered. Unable to update tags.") final String id,
        @NotEmpty(message = "No tags entered. Unable to update.") final Set<String> tags
    ) throws GenieException {
        this.findCommand(id).setTags(this.createAndGetTagEntities(tags));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllTagsForCommand(
        @NotBlank(message = "No command id entered. Unable to remove tags.") final String id
    ) throws GenieException {
        this.findCommand(id).getTags().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeTagForCommand(
        @NotBlank(message = "No command id entered. Unable to remove tag.") final String id,
        @NotBlank(message = "No tag entered. Unable to remove.") final String tag
    ) throws GenieException {
        this.getTagPersistenceService().getTag(tag).ifPresent(this.findCommand(id).getTags()::remove);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addApplicationsForCommand(
        @NotBlank(message = "No command id entered. Unable to add applications.") final String id,
        @NotEmpty(message = "No application ids entered. Unable to add applications.") final List<String> applicationIds
    ) throws GenieException {
        if (applicationIds.size()
            != applicationIds.stream().filter(this.getApplicationRepository()::existsByUniqueId).count()) {
            throw new GeniePreconditionException("All applications need to exist to add to a command");
        }

        final CommandEntity commandEntity = this.findCommand(id);
        for (final String appId : applicationIds) {
            commandEntity.addApplication(
                this.getApplicationEntity(appId)
                    .orElseThrow(() -> new GenieNotFoundException("No application with id " + appId + " found"))
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setApplicationsForCommand(
        @NotBlank(message = "No command id entered. Unable to set applications.") final String id,
        @NotNull(message = "No application ids entered. Unable to set applications.") final List<String> applicationIds
    ) throws GenieException {
        if (applicationIds.size()
            != applicationIds.stream().filter(this.getApplicationRepository()::existsByUniqueId).count()) {
            throw new GeniePreconditionException("All applications need to exist to add to a command");
        }

        final CommandEntity commandEntity = this.findCommand(id);
        final List<ApplicationEntity> applicationEntities = new ArrayList<>();
        for (final String appId : applicationIds) {
            applicationEntities.add(
                this.getApplicationEntity(appId).orElseThrow(
                    () -> new GenieNotFoundException("Couldn't find application with unique id " + appId)
                )
            );
        }

        commandEntity.setApplications(applicationEntities);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Application> getApplicationsForCommand(
        @NotBlank(message = "No command id entered. Unable to get applications.") final String id
    ) throws GenieException {
        final CommandEntity commandEntity = this.findCommand(id);
        return commandEntity
            .getApplications()
            .stream()
            .map(EntityDtoConverters::toV4ApplicationDto)
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeApplicationsForCommand(
        @NotBlank(message = "No command id entered. Unable to remove applications.") final String id
    ) throws GenieException {
        this.findCommand(id).setApplications(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeApplicationForCommand(
        @NotBlank(message = "No command id entered. Unable to remove application.") final String id,
        @NotBlank(message = "No application id entered. Unable to remove application.") final String appId
    ) throws GenieException {
        this.getApplicationRepository().findByUniqueId(appId).ifPresent(this.findCommand(id).getApplications()::remove);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<Cluster> getClustersForCommand(
        @NotBlank(message = "No command id entered. Unable to get clusters.") final String id,
        @Nullable final Set<ClusterStatus> statuses
    ) throws GenieException {
        if (!this.getCommandRepository().existsByUniqueId(id)) {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
        return this.getClusterRepository()
            .findAll(JpaClusterSpecs.findClustersForCommand(id, statuses))
            .stream()
            .map(EntityDtoConverters::toV4ClusterDto)
            .collect(Collectors.toSet());
    }

    /**
     * Helper method to find a command entity.
     *
     * @param id The id of the command to find
     * @return The command entity if one is found
     * @throws GenieNotFoundException When the command doesn't exist
     */
    private CommandEntity findCommand(final String id) throws GenieNotFoundException {
        return this.getCommandRepository()
            .findByUniqueId(id)
            .orElseThrow(() -> new GenieNotFoundException("No command with id " + id + " exists."));
    }

    private CommandEntity createCommandEntity(final CommandRequest request) {
        final ExecutionEnvironment resources = request.getResources();
        final CommandMetadata metadata = request.getMetadata();

        final CommandEntity entity = new CommandEntity();
        this.setUniqueId(entity, request.getRequestedId().orElse(null));
        entity.setCheckDelay(request.getCheckDelay().orElse(com.netflix.genie.common.dto.Command.DEFAULT_CHECK_DELAY));
        entity.setExecutable(request.getExecutable());
        request.getMemory().ifPresent(entity::setMemory);
        this.setEntityResources(resources, entity::setConfigs, entity::setDependencies, entity::setSetupFile);
        this.setEntityTags(metadata.getTags(), entity::setTags);
        this.setEntityCommandMetadata(entity, metadata);

        return entity;
    }

    private void updateEntityWithDtoContents(final CommandEntity entity, final Command dto) {
        final ExecutionEnvironment resources = dto.getResources();
        final CommandMetadata metadata = dto.getMetadata();

        // Save all the unowned entities first to avoid unintended flushes
        this.setEntityResources(resources, entity::setConfigs, entity::setDependencies, entity::setSetupFile);
        this.setEntityTags(metadata.getTags(), entity::setTags);
        this.setEntityCommandMetadata(entity, metadata);

        entity.setCheckDelay(dto.getCheckDelay());
        entity.setExecutable(dto.getExecutable());
        entity.setMemory(dto.getMemory().orElse(null));
    }

    // TODO: Try to reuse code here once big bang changes are done

    private void setEntityCommandMetadata(final CommandEntity entity, final CommandMetadata metadata) {
        // NOTE: These are all called in case someone has changed it to set something to null. DO NOT use ifPresent
        entity.setName(metadata.getName());
        entity.setUser(metadata.getUser());
        entity.setVersion(metadata.getVersion());
        entity.setDescription(metadata.getDescription().orElse(null));
        entity.setStatus(metadata.getStatus());
        EntityDtoConverters.setJsonField(metadata.getMetadata().orElse(null), entity::setMetadata);
    }
}
