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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jpa.entities.ApplicationEntity;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.entities.FileEntity;
import com.netflix.genie.core.jpa.entities.TagEntity;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaFileRepository;
import com.netflix.genie.core.jpa.repositories.JpaTagRepository;
import com.netflix.genie.core.jpa.specifications.JpaClusterSpecs;
import com.netflix.genie.core.jpa.specifications.JpaCommandSpecs;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.FileService;
import com.netflix.genie.core.services.TagService;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Implementation of the CommandService interface.
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
public class JpaCommandServiceImpl extends JpaBaseService implements CommandService {

    private final ObjectMapper mapper = new ObjectMapper();
    private final JpaCommandRepository commandRepository;
    private final JpaApplicationRepository applicationRepository;
    private final JpaClusterRepository clusterRepo;

    /**
     * Default constructor.
     *
     * @param tagService            The tag service to use
     * @param tagRepository         The tag repository to use
     * @param fileService           The file service to use
     * @param fileRepository        The file repository to use
     * @param commandRepository     the command repository to use
     * @param applicationRepository the application repository to use
     * @param clusterRepository     the cluster repository to use
     */
    public JpaCommandServiceImpl(
        final TagService tagService,
        final JpaTagRepository tagRepository,
        final FileService fileService,
        final JpaFileRepository fileRepository,
        final JpaCommandRepository commandRepository,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository
    ) {
        super(tagService, tagRepository, fileService, fileRepository);
        this.commandRepository = commandRepository;
        this.applicationRepository = applicationRepository;
        this.clusterRepo = clusterRepository;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createCommand(
        @NotNull(message = "No command entered. Unable to create.")
        @Valid final Command command
    ) throws GenieException {
        log.debug("Called to create command {}", command);
        final Optional<String> commandId = command.getId();
        if (commandId.isPresent() && this.commandRepository.existsByUniqueId(commandId.get())) {
            throw new GenieConflictException("A command with id " + commandId.get() + " already exists");
        }

        final CommandEntity commandEntity = new CommandEntity();
        commandEntity.setUniqueId(command.getId().orElse(UUID.randomUUID().toString()));
        this.updateEntityWithDtoContents(commandEntity, command);
        this.commandRepository.save(commandEntity);
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
        return JpaServiceUtils.toCommandDto(this.findCommand(id));
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
            tagEntities = this.getTagRepository().findByTagIn(tags);
            if (tagEntities.size() != tags.size()) {
                return new PageImpl<>(new ArrayList<Command>(), page, 0);
            }
        } else {
            tagEntities = null;
        }

        @SuppressWarnings("unchecked") final Page<CommandEntity> commandEntities = this.commandRepository.findAll(
            JpaCommandSpecs.find(
                name,
                user,
                statuses,
                tagEntities
            ),
            page
        );

        return commandEntities.map(JpaServiceUtils::toCommandDto);
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
        if (!this.commandRepository.existsByUniqueId(id)) {
            throw new GenieNotFoundException("No command exists with the given id. Unable to update.");
        }
        final Optional<String> updateId = updateCommand.getId();
        if (updateId.isPresent() && !id.equals(updateId.get())) {
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
            final Command commandToPatch = JpaServiceUtils.toCommandDto(commandEntity);
            log.debug("Will patch command {}. Original state: {}", id, commandToPatch);
            final JsonNode commandNode = this.mapper.readTree(commandToPatch.toString());
            final JsonNode postPatchNode = patch.apply(commandNode);
            final Command patchedCommand = this.mapper.treeToValue(postPatchNode, Command.class);
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
        for (final CommandEntity commandEntity : this.commandRepository.findAll()) {
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
        this.commandRepository.delete(commandEntity);
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
        this.getFileRepository().findByFile(config).ifPresent(this.findCommand(id).getConfigs()::remove);
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
        this.getFileRepository().findByFile(dependency).ifPresent(this.findCommand(id).getDependencies()::remove);
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
        final CommandEntity commandEntity = this.findCommand(id);
        final Set<TagEntity> newTags = this.createAndGetTagEntities(tags);
        this.setFinalTags(newTags, commandEntity.getUniqueId(), commandEntity.getName());
        commandEntity.setTags(newTags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllTagsForCommand(
        @NotBlank(message = "No command id entered. Unable to remove tags.") final String id
    ) throws GenieException {
        final Set<TagEntity> tags = this.findCommand(id).getTags();
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
    public void removeTagForCommand(
        @NotBlank(message = "No command id entered. Unable to remove tag.") final String id,
        @NotBlank(message = "No tag entered. Unable to remove.") final String tag
    ) throws GenieException {
        if (!tag.startsWith(JpaBaseService.GENIE_TAG_NAMESPACE)) {
            this.getTagRepository().findByTag(tag).ifPresent(this.findCommand(id).getTags()::remove);
        }
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
            != applicationIds.stream().filter(this.applicationRepository::existsByUniqueId).count()) {
            throw new GeniePreconditionException("All applications need to exist to add to a command");
        }

        final CommandEntity commandEntity = this.findCommand(id);
        for (final String appId : applicationIds) {
            commandEntity.addApplication(
                this.applicationRepository
                    .findByUniqueId(appId)
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
            != applicationIds.stream().filter(this.applicationRepository::existsByUniqueId).count()) {
            throw new GeniePreconditionException("All applications need to exist to add to a command");
        }

        final CommandEntity commandEntity = this.findCommand(id);
        final List<ApplicationEntity> applicationEntities = new ArrayList<>();
        for (final String appId : applicationIds) {
            applicationEntities.add(
                this.applicationRepository
                    .findByUniqueId(appId)
                    .orElseThrow(
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
            .map(JpaServiceUtils::toApplicationDto)
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
        this.applicationRepository.findByUniqueId(appId).ifPresent(this.findCommand(id).getApplications()::remove);
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
        if (!this.commandRepository.existsByUniqueId(id)) {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
        @SuppressWarnings("unchecked") final List<ClusterEntity> clusterEntities = this.clusterRepo.findAll(
            JpaClusterSpecs.findClustersForCommand(
                id,
                statuses
            )
        );

        return clusterEntities
            .stream()
            .map(JpaServiceUtils::toClusterDto)
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
        return this.commandRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new GenieNotFoundException("No command with id " + id + " exists."));
    }

    private void updateEntityWithDtoContents(
        final CommandEntity entity,
        final Command dto
    ) throws GenieException {
        // NOTE: These are all called in case someone has changed it to set something to null. DO NOT use ifPresent
        entity.setName(dto.getName());
        entity.setUser(dto.getUser());
        entity.setVersion(dto.getVersion());
        entity.setDescription(dto.getDescription().orElse(null));
        entity.setExecutable(dto.getExecutable());
        entity.setCheckDelay(dto.getCheckDelay());
        entity.setConfigs(this.createAndGetFileEntities(dto.getConfigs()));
        entity.setDependencies(this.createAndGetFileEntities(dto.getDependencies()));
        final FileEntity setupFile = dto.getSetupFile().isPresent()
            ? this.createAndGetFileEntity(dto.getSetupFile().get())
            : null;
        entity.setSetupFile(setupFile);
        entity.setStatus(dto.getStatus());
        entity.setTags(this.createAndGetTagEntities(dto.getTags()));
        entity.setMemory(dto.getMemory().orElse(null));

        this.setFinalTags(entity.getTags(), entity.getUniqueId(), entity.getName());
    }
}
