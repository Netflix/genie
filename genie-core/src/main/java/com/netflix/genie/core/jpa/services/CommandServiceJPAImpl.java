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
import com.netflix.genie.common.model.Command_;
import com.netflix.genie.core.jpa.repositories.ApplicationRepository;
import com.netflix.genie.core.jpa.repositories.ClusterRepository;
import com.netflix.genie.core.jpa.repositories.ClusterSpecs;
import com.netflix.genie.core.jpa.repositories.CommandRepository;
import com.netflix.genie.core.jpa.repositories.CommandSpecs;
import com.netflix.genie.core.services.CommandService;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
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
public class CommandServiceJPAImpl implements CommandService {

    private static final Logger LOG = LoggerFactory.getLogger(CommandServiceJPAImpl.class);
    private final CommandRepository commandRepo;
    private final ApplicationRepository appRepo;
    private final ClusterRepository clusterRepo;

    /**
     * Default constructor.
     *
     * @param commandRepo the command repository to use
     * @param appRepo     the application repository to use
     * @param clusterRepo the cluster repository to use
     */
    @Autowired
    public CommandServiceJPAImpl(
            final CommandRepository commandRepo,
            final ApplicationRepository appRepo,
            final ClusterRepository clusterRepo
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to create command " + command.toString());
        }
        if (StringUtils.isNotBlank(command.getId()) && this.commandRepo.exists(command.getId())) {
            throw new GenieConflictException("A command with id " + command.getId() + " already exists");
        }

        final com.netflix.genie.common.model.Command commandEntity = new com.netflix.genie.common.model.Command();
        commandEntity.setId(StringUtils.isBlank(command.getId()) ? UUID.randomUUID().toString() : command.getId());
        commandEntity.setName(command.getName());
        commandEntity.setUser(command.getUser());
        commandEntity.setVersion(command.getVersion());
        commandEntity.setDescription(command.getDescription());
        commandEntity.setExecutable(command.getExecutable());
        commandEntity.setConfigs(command.getConfigs());
        commandEntity.setJobType(command.getJobType());
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("called");
        }
        return this.findCommand(id).getDTO();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Command> getCommands(
            final String name,
            final String userName,
            final Set<CommandStatus> statuses,
            final Set<String> tags,
            final int page,
            final int limit,
            final boolean descending,
            final Set<String> orderBys) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called");
        }

        final PageRequest pageRequest = JPAUtils.getPageRequest(
                page, limit, descending, orderBys, Command_.class, Command_.updated.getName()
        );

        @SuppressWarnings("unchecked")
        final List<com.netflix.genie.common.model.Command> commands = this.commandRepo.findAll(
                CommandSpecs.find(
                        name,
                        userName,
                        statuses,
                        tags
                ),
                pageRequest).getContent();

        return commands.stream().map(com.netflix.genie.common.model.Command::getDTO).collect(Collectors.toList());
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to update command with id " + id + " " + updateCommand.toString());
        }

        final com.netflix.genie.common.model.Command savedCommand = this.findCommand(id);
        savedCommand.setName(updateCommand.getName());
        savedCommand.setUser(updateCommand.getUser());
        savedCommand.setVersion(updateCommand.getVersion());
        savedCommand.setStatus(updateCommand.getStatus());
        savedCommand.setDescription(updateCommand.getDescription());
        savedCommand.setExecutable(updateCommand.getExecutable());
        savedCommand.setSetupFile(updateCommand.getSetupFile());
        savedCommand.setJobType(updateCommand.getJobType());
        savedCommand.setConfigs(updateCommand.getConfigs());
        savedCommand.setTags(updateCommand.getTags());
        this.commandRepo.save(savedCommand);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllCommands() throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to delete all commands");
        }
        for (final com.netflix.genie.common.model.Command command : this.commandRepo.findAll()) {
            this.deleteCommand(command.getId());
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to delete command config with id " + id);
        }
        final com.netflix.genie.common.model.Command command = this.findCommand(id);

        //Remove the command from the associated Application references
        final Set<com.netflix.genie.common.model.Application> apps = command.getApplications();
        if (apps != null) {
            apps.stream()
                    .filter(application -> application.getCommands() != null)
                    .forEach(
                            application -> {
                                application.getCommands().remove(command);
                                this.appRepo.save(application);
                            }
                    );
        }
        //Remove the command from the associated cluster references
        final Set<com.netflix.genie.common.model.Cluster> clusters = command.getClusters();
        for (final com.netflix.genie.common.model.Cluster cluster : clusters) {
            final List<com.netflix.genie.common.model.Command> commands = cluster.getCommands();
            if (commands != null) {
                commands.remove(command);
            }
            this.clusterRepo.save(cluster);
        }
        this.commandRepo.delete(command);
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
        this.findCommand(id).getTags().addAll(tags);
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
        this.findCommand(id).getTags().clear();
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
        this.findCommand(id).getTags().remove(tag);
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

        final com.netflix.genie.common.model.Command command = this.findCommand(id);
        applicationIds.stream().forEach(
                applicationId -> command.getApplications().add(this.appRepo.findOne(applicationId))
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

        final com.netflix.genie.common.model.Command command = this.findCommand(id);
        command.getApplications().clear();
        applicationIds
                .stream()
                .forEach(applicationId -> command.getApplications().add(this.appRepo.findOne(applicationId)));
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
        final com.netflix.genie.common.model.Command command = this.findCommand(id);
        return command
                .getApplications()
                .stream()
                .map(com.netflix.genie.common.model.Application::getDTO)
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
        final com.netflix.genie.common.model.Command command = this.findCommand(id);
        final com.netflix.genie.common.model.Application application = this.appRepo.findOne(appId);
        if (application != null) {
            command.getApplications().remove(application);
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
        final List<com.netflix.genie.common.model.Cluster> clusters = this.clusterRepo.findAll(
                ClusterSpecs.findClustersForCommand(
                        id,
                        statuses
                )
        );

        return clusters.stream().map(com.netflix.genie.common.model.Cluster::getDTO).collect(Collectors.toSet());
    }

    /**
     * Helper method to find a command.
     *
     * @param id The id of the command to find
     * @return The command if one is found
     * @throws GenieNotFoundException When the command doesn't exist
     */
    private com.netflix.genie.common.model.Command findCommand(final String id) throws GenieNotFoundException {
        final com.netflix.genie.common.model.Command command = this.commandRepo.findOne(id);
        if (command != null) {
            return command;
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }
}
