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
package com.netflix.genie.server.services.impl.jpa;

import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.common.model.ClusterStatus;
import com.netflix.genie.common.model.Command_;

import com.netflix.genie.server.repository.jpa.CommandRepository;
import com.netflix.genie.server.repository.jpa.ApplicationRepository;
import com.netflix.genie.server.repository.jpa.ClusterRepository;
import com.netflix.genie.server.repository.jpa.CommandSpecs;
import com.netflix.genie.server.repository.jpa.ClusterSpecs;
import com.netflix.genie.server.services.CommandConfigService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;

/**
 * Implementation of the CommandConfigService interface.
 *
 * @author amsharma
 * @author tgianos
 */
@Transactional(
        rollbackFor = {
                GenieException.class,
                ConstraintViolationException.class
        }
)
public class CommandConfigServiceJPAImpl implements CommandConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(CommandConfigServiceJPAImpl.class);
    private final CommandRepository commandRepo;
    private final ApplicationRepository appRepo;
    private final ClusterRepository clusterRepo;
    @PersistenceContext
    private EntityManager em;

    /**
     * Default constructor.
     *
     * @param commandRepo the command repository to use
     * @param appRepo     the application repository to use
     * @param clusterRepo the cluster repository to use
     */
    public CommandConfigServiceJPAImpl(
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
    public Command createCommand(
            @NotNull(message = "No command entered. Unable to create.")
            @Valid
            final Command command
    ) throws GenieException {
        LOG.debug("Called to create command " + command.toString());
        if (StringUtils.isEmpty(command.getId())) {
            command.setId(UUID.randomUUID().toString());
        }
        if (this.commandRepo.exists(command.getId())) {
            throw new GenieConflictException("A command with id " + command.getId() + " already exists");
        }

        return this.commandRepo.save(command);
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
        LOG.debug("called");
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            return command;
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
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
        LOG.debug("Called");

        final PageRequest pageRequest = JPAUtils.getPageRequest(
                page, limit, descending, orderBys, Command_.class, Command_.updated.getName()
        );

        @SuppressWarnings("unchecked")
        final List<Command> commands = this.commandRepo.findAll(
                CommandSpecs.find(
                        name,
                        userName,
                        statuses,
                        tags
                ),
                pageRequest).getContent();
        return commands;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Command updateCommand(
            @NotBlank(message = "No id entered. Unable to update.")
            final String id,
            @NotNull(message = "No command information entered. Unable to update.")
            final Command updateCommand
    ) throws GenieException {
        if (!this.commandRepo.exists(id)) {
            throw new GenieNotFoundException("No command exists with the given id. Unable to update.");
        }
        if (StringUtils.isNotBlank(updateCommand.getId())
                && !id.equals(updateCommand.getId())) {
            throw new GenieBadRequestException("Command id inconsistent with id passed in.");
        }

        //Set the id if it's not set so we can merge
        if (StringUtils.isBlank(updateCommand.getId())) {
            updateCommand.setId(id);
        }
        LOG.debug("Called to update command with id " + id + " " + updateCommand.toString());
        return this.em.merge(updateCommand);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Command> deleteAllCommands() throws GenieException {
        LOG.debug("Called to delete all commands");
        final Iterable<Command> commands = this.commandRepo.findAll();
        final List<Command> returnCommands = new ArrayList<>();
        for (final Command command : commands) {
            returnCommands.add(this.deleteCommand(command.getId()));
        }
        return returnCommands;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Command deleteCommand(
            @NotBlank(message = "No id entered. Unable to delete.")
            final String id
    ) throws GenieException {
        LOG.debug("Called to delete command config with id " + id);
        final Command command = this.commandRepo.findOne(id);
        if (command == null) {
            throw new GenieNotFoundException("No command with id " + id + " exists to delete.");
        }
        //Remove the command from the associated Application references
        final Application app = command.getApplication();
        if (app != null) {
            final Set<Command> commands = app.getCommands();
            if (commands != null) {
                commands.remove(command);
            }
        }
        this.commandRepo.delete(command);
        return command;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> addConfigsForCommand(
            @NotBlank(message = "No command id entered. Unable to add configurations.")
            final String id,
            @NotEmpty(message = "No configuration files entered. Unable to add.")
            final Set<String> configs
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.getConfigs().addAll(configs);
            return command.getConfigs();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
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
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            return command.getConfigs();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> updateConfigsForCommand(
            @NotBlank(message = "No command id entered. Unable to update configurations.")
            final String id,
            @NotEmpty(message = "No configs entered. Unable to update.")
            final Set<String> configs
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.setConfigs(configs);
            return command.getConfigs();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeAllConfigsForCommand(
            @NotBlank(message = "No command id entered. Unable to remove configs.")
            final String id
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.getConfigs().clear();
            return command.getConfigs();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeConfigForCommand(
            @NotBlank(message = "No command id entered. Unable to remove configuration.")
            final String id,
            @NotBlank(message = "No config entered. Unable to remove.")
            final String config
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            if (StringUtils.isNotBlank(config)) {
                command.getConfigs().remove(config);
            }
            return command.getConfigs();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Application setApplicationForCommand(
            @NotBlank(message = "No command id entered. Unable to add applications.")
            final String id,
            @NotNull(message = "No application entered. Unable to set application.")
            final Application application
    ) throws GenieException {
        if (StringUtils.isBlank(application.getId())) {
            throw new GeniePreconditionException("No application id entered. Unable to set application.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            final Application app = this.appRepo.findOne(application.getId());
            if (app != null) {
                command.setApplication(app);
            } else {
                throw new GenieNotFoundException("No application with id " + application.getId() + " exists.");
            }
            return command.getApplication();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Application getApplicationForCommand(
            @NotBlank(message = "No command id entered. Unable to get applications.")
            final String id
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            final Application app = command.getApplication();
            if (app != null) {
                return app;
            } else {
                throw new GenieNotFoundException("No application set for command with id '" + id + "'.");
            }
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Application removeApplicationForCommand(
            @NotBlank(message = "No command id entered. Unable to remove application.")
            final String id
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            final Application app = command.getApplication();
            if (app != null) {
                command.setApplication(null);
                return app;
            } else {
                throw new GenieNotFoundException("No application set for command with id '" + id + "'.");
            }
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> addTagsForCommand(
            @NotBlank(message = "No command id entered. Unable to add tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to add.")
            final Set<String> tags
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.getTags().addAll(tags);
            return command.getTags();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
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
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            return command.getTags();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> updateTagsForCommand(
            @NotBlank(message = "No command id entered. Unable to update tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to update.")
            final Set<String> tags
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.setTags(tags);
            return command.getTags();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeAllTagsForCommand(
            @NotBlank(message = "No command id entered. Unable to remove tags.")
            final String id
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.getTags().clear();
            return command.getTags();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeTagForCommand(
            @NotBlank(message = "No command id entered. Unable to remove tag.")
            final String id,
            @NotBlank(message = "No tag entered. Unable to remove.")
            final String tag
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.getTags().remove(tag);
            return command.getTags();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Cluster> getClustersForCommand(
            @NotBlank(message = "No command id entered. Unable to get clusters.")
            final String id,
            final Set<ClusterStatus> statuses
    ) throws GenieException {
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            @SuppressWarnings("unchecked")
            final List<Cluster> clusters = this.clusterRepo.findAll(
                    ClusterSpecs.findClustersForCommand(
                            id,
                            statuses
                    ));
            return clusters;
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }
}
