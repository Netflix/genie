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
import com.netflix.genie.core.jpa.entities.ApplicationEntity;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.specifications.JpaClusterSpecs;
import com.netflix.genie.core.jpa.specifications.JpaCommandSpecs;
import com.netflix.genie.core.services.CommandService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Implementation of the CommandService interface.
 *
 * @author amsharma
 * @author tgianos
 */
@Service
@Transactional(
    rollbackFor = {
        GenieException.class,
        ConstraintViolationException.class
    }
)
@Slf4j
public class JpaCommandServiceImpl implements CommandService {

    private final JpaCommandRepository commandRepo;
    private final JpaApplicationRepository appRepo;
    private final JpaClusterRepository clusterRepo;

    /**
     * Default constructor.
     *
     * @param commandRepo the command repository to use
     * @param appRepo     the application repository to use
     * @param clusterRepo the cluster repository to use
     */
    @Autowired
    public JpaCommandServiceImpl(
        final JpaCommandRepository commandRepo,
        final JpaApplicationRepository appRepo,
        final JpaClusterRepository clusterRepo
    ) {
        this.commandRepo = commandRepo;
        this.appRepo = appRepo;
        this.clusterRepo = clusterRepo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createCommand(
        @NotNull(message = "No command entered. Unable to create.")
        @Valid
        final Command command
    ) throws GenieException {
        log.debug("Called to create command {}", command);
        if (StringUtils.isNotBlank(command.getId()) && this.commandRepo.exists(command.getId())) {
            throw new GenieConflictException("A command with id " + command.getId() + " already exists");
        }

        final CommandEntity commandEntity = new CommandEntity();
        commandEntity.setId(StringUtils.isBlank(command.getId()) ? UUID.randomUUID().toString() : command.getId());
        commandEntity.setName(command.getName());
        commandEntity.setUser(command.getUser());
        commandEntity.setVersion(command.getVersion());
        commandEntity.setDescription(command.getDescription());
        commandEntity.setExecutable(command.getExecutable());
        commandEntity.setCheckDelay(command.getCheckDelay());
        commandEntity.setConfigs(command.getConfigs());
        commandEntity.setSetupFile(command.getSetupFile());
        commandEntity.setStatus(command.getStatus());
        commandEntity.setTags(command.getTags());

        return this.commandRepo.save(commandEntity).getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Command getCommand(
        @NotBlank(message = "No id entered unable to get.")
        final String id
    ) throws GenieException {
        log.debug("called");
        return this.findCommand(id).getDTO();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Command> getCommands(
        final String name,
        final String userName,
        final Set<CommandStatus> statuses,
        final Set<String> tags,
        final Pageable page
    ) {
        log.debug("Called");

        @SuppressWarnings("unchecked")
        final Page<CommandEntity> commandEntities = this.commandRepo.findAll(
            JpaCommandSpecs.find(
                name,
                userName,
                statuses,
                tags
            ),
            page
        );

        return commandEntities.map(CommandEntity::getDTO);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCommand(
        @NotBlank(message = "No id entered. Unable to update.")
        final String id,
        @NotNull(message = "No command information entered. Unable to update.")
        @Valid
        final Command updateCommand
    ) throws GenieException {
        if (!this.commandRepo.exists(id)) {
            throw new GenieNotFoundException("No command exists with the given id. Unable to update.");
        }
        if (!id.equals(updateCommand.getId())) {
            throw new GenieBadRequestException("Command id inconsistent with id passed in.");
        }

        log.debug("Called to update command with id {} {}", id, updateCommand);

        final CommandEntity commandEntity = this.findCommand(id);
        commandEntity.setName(updateCommand.getName());
        commandEntity.setUser(updateCommand.getUser());
        commandEntity.setVersion(updateCommand.getVersion());
        commandEntity.setStatus(updateCommand.getStatus());
        commandEntity.setDescription(updateCommand.getDescription());
        commandEntity.setExecutable(updateCommand.getExecutable());
        commandEntity.setCheckDelay(updateCommand.getCheckDelay());
        commandEntity.setSetupFile(updateCommand.getSetupFile());
        commandEntity.setConfigs(updateCommand.getConfigs());
        commandEntity.setTags(updateCommand.getTags());
        this.commandRepo.save(commandEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllCommands() throws GenieException {
        log.debug("Called to delete all commands");
        for (final CommandEntity commandEntity : this.commandRepo.findAll()) {
            this.deleteCommand(commandEntity.getId());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteCommand(
        @NotBlank(message = "No id entered. Unable to delete.")
        final String id
    ) throws GenieException {
        log.debug("Called to delete command config with id {}");
        final CommandEntity commandEntity = this.findCommand(id);

        //Remove the command from the associated Application references
        final Set<ApplicationEntity> applicationEntities
            = commandEntity.getApplications();
        if (applicationEntities != null) {
            applicationEntities.stream()
                .filter(application -> application.getCommands() != null)
                .forEach(
                    application -> {
                        application.getCommands().remove(commandEntity);
                        this.appRepo.save(application);
                    }
                );
        }
        //Remove the command from the associated cluster references
        final Set<ClusterEntity> clusterEntities = commandEntity.getClusters();
        for (final ClusterEntity clusterEntity : clusterEntities) {
            final List<CommandEntity> commandEntities = clusterEntity.getCommands();
            if (commandEntities != null) {
                commandEntities.remove(commandEntity);
            }
            this.clusterRepo.save(clusterEntity);
        }
        this.commandRepo.delete(commandEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addConfigsForCommand(
        @NotBlank(message = "No command id entered. Unable to add configurations.")
        final String id,
        @NotEmpty(message = "No configuration files entered. Unable to add.")
        final Set<String> configs
    ) throws GenieException {
        this.findCommand(id).getConfigs().addAll(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForCommand(
        @NotBlank(message = "No command id entered. Unable to get configs.")
        final String id
    ) throws GenieException {
        return this.findCommand(id).getConfigs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateConfigsForCommand(
        @NotBlank(message = "No command id entered. Unable to update configurations.")
        final String id,
        @NotEmpty(message = "No configs entered. Unable to update.")
        final Set<String> configs
    ) throws GenieException {
        this.findCommand(id).setConfigs(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllConfigsForCommand(
        @NotBlank(message = "No command id entered. Unable to remove configs.")
        final String id
    ) throws GenieException {
        this.findCommand(id).getConfigs().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeConfigForCommand(
        @NotBlank(message = "No command id entered. Unable to remove configuration.")
        final String id,
        @NotBlank(message = "No config entered. Unable to remove.")
        final String config
    ) throws GenieException {
        this.findCommand(id).getConfigs().remove(config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addTagsForCommand(
        @NotBlank(message = "No command id entered. Unable to add tags.")
        final String id,
        @NotEmpty(message = "No tags entered. Unable to add.")
        final Set<String> tags
    ) throws GenieException {
        final CommandEntity command = this.findCommand(id);
        final Set<String> commandTags = command.getTags();
        commandTags.addAll(tags);
        command.setTags(commandTags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForCommand(
        @NotBlank(message = "No command id sent. Cannot retrieve tags.")
        final String id
    ) throws GenieException {
        return this.findCommand(id).getTags();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTagsForCommand(
        @NotBlank(message = "No command id entered. Unable to update tags.")
        final String id,
        @NotEmpty(message = "No tags entered. Unable to update.")
        final Set<String> tags
    ) throws GenieException {
        this.findCommand(id).setTags(tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllTagsForCommand(
        @NotBlank(message = "No command id entered. Unable to remove tags.")
        final String id
    ) throws GenieException {
        this.findCommand(id).setTags(Sets.newHashSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeTagForCommand(
        @NotBlank(message = "No command id entered. Unable to remove tag.")
        final String id,
        @NotBlank(message = "No tag entered. Unable to remove.")
        final String tag
    ) throws GenieException {
        final CommandEntity command = this.findCommand(id);
        final Set<String> commandTags = command.getTags();
        commandTags.remove(tag);
        command.setTags(commandTags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addApplicationsForCommand(
        @NotBlank(message = "No command id entered. Unable to add applications.")
        final String id,
        @NotEmpty(message = "No application ids entered. Unable to add applications.")
        final Set<String> applicationIds
    ) throws GenieException {
        if (applicationIds.size() != applicationIds.stream().filter(this.appRepo::exists).count()) {
            throw new GeniePreconditionException("All applications need to exist to add to a command");
        }

        final CommandEntity commandEntity = this.findCommand(id);
        applicationIds.stream().forEach(
            applicationId -> commandEntity.getApplications().add(this.appRepo.findOne(applicationId))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setApplicationsForCommand(
        @NotBlank(message = "No command id entered. Unable to set applications.")
        final String id,
        @NotNull(message = "No application ids entered. Unable to set applications.")
        final Set<String> applicationIds
    ) throws GenieException {
        if (applicationIds.size() != applicationIds.stream().filter(this.appRepo::exists).count()) {
            throw new GeniePreconditionException("All applications need to exist to add to a command");
        }

        final CommandEntity commandEntity = this.findCommand(id);
        commandEntity.getApplications().clear();
        applicationIds
            .stream()
            .forEach(applicationId -> commandEntity.getApplications().add(this.appRepo.findOne(applicationId)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<Application> getApplicationsForCommand(
        @NotBlank(message = "No command id entered. Unable to get applications.")
        final String id
    ) throws GenieException {
        final CommandEntity commandEntity = this.findCommand(id);
        return commandEntity
            .getApplications()
            .stream()
            .map(ApplicationEntity::getDTO)
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeApplicationsForCommand(
        @NotBlank(message = "No command id entered. Unable to remove applications.")
        final String id
    ) throws GenieException {
        this.findCommand(id).setApplications(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeApplicationForCommand(
        @NotBlank(message = "No command id entered. Unable to remove application.")
        final String id,
        @NotBlank(message = "No application id entered. Unable to remove application.")
        final String appId
    ) throws GenieException {
        final CommandEntity commandEntity = this.findCommand(id);
        final ApplicationEntity applicationEntity = this.appRepo.findOne(appId);
        if (applicationEntity != null) {
            commandEntity.getApplications().remove(applicationEntity);
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<Cluster> getClustersForCommand(
        @NotBlank(message = "No command id entered. Unable to get clusters.")
        final String id,
        final Set<ClusterStatus> statuses
    ) throws GenieException {
        if (!this.commandRepo.exists(id)) {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
        @SuppressWarnings("unchecked")
        final List<ClusterEntity> clusterEntities = this.clusterRepo.findAll(
            JpaClusterSpecs.findClustersForCommand(
                id,
                statuses
            )
        );

        return clusterEntities
            .stream()
            .map(ClusterEntity::getDTO)
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
        final CommandEntity commandEntity = this.commandRepo.findOne(id);
        if (commandEntity != null) {
            return commandEntity;
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }
}
