/*
 *
 *  Copyright 2014 Netflix, Inc.
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
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Command_;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.server.repository.jpa.ApplicationRepository;
import com.netflix.genie.server.repository.jpa.CommandRepository;
import com.netflix.genie.server.repository.jpa.CommandSpecs;
import com.netflix.genie.server.services.CommandConfigService;
import org.springframework.data.domain.Sort.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.commons.lang3.StringUtils;
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
@Named
@Transactional(rollbackFor = GenieException.class)
public class CommandConfigServiceJPAImpl implements CommandConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(CommandConfigServiceJPAImpl.class);

    @PersistenceContext
    private EntityManager em;

    private final CommandRepository commandRepo;
    private final ApplicationRepository appRepo;

    /**
     * Default constructor.
     *
     * @param commandRepo the command repository to use
     * @param appRepo     the application repository to use
     */
    @Inject
    public CommandConfigServiceJPAImpl(
            final CommandRepository commandRepo,
            final ApplicationRepository appRepo) {
        this.commandRepo = commandRepo;
        this.appRepo = appRepo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Command createCommand(final Command command) throws GenieException {
        if (command == null) {
            throw new GeniePreconditionException("No command entered to create");
        }
        command.validate();

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
    public Command getCommand(final String id) throws GenieException {
        LOG.debug("called");
        if (StringUtils.isEmpty(id)) {
            throw new GeniePreconditionException("Id can't be null or empty.");
        }
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
            final int limit) {
        LOG.debug("Called");

        final PageRequest pageRequest = new PageRequest(
                page < 0 ? 0 : page,
                limit < 1 ? 1024 : limit,
                Direction.DESC,
                Command_.updated.getName()
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
            final String id,
            final Command updateCommand) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No id entered. Unable to update.");
        }
        if (updateCommand == null) {
            throw new GeniePreconditionException("No command information entered. Unable to update.");
        }
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
        final Command command = this.em.merge(updateCommand);
        command.validate();
        return command;
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
    public Command deleteCommand(final String id) throws GenieException {
        LOG.debug("Called to delete command config with id " + id);
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No id entered. Unable to delete.");
        }
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
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to add configurations.");
        }
        if (configs == null) {
            throw new GeniePreconditionException("No configuration files entered.");
        }
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
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to get configs.");
        }
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
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to update configurations.");
        }
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
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to remove configs.");
        }
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
            final String id,
            final String config) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to remove configuration.");
        }
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
            final String id,
            final Application application) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to add applications.");
        }
        if (application == null) {
            throw new GeniePreconditionException("No application entered. Unable to set application.");
        }
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
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to get applications.");
        }
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
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to remove application.");
        }
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
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to add tags.");
        }
        if (tags == null) {
            throw new GeniePreconditionException("No tags entered.");
        }
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
            final String id)
            throws GenieException {

        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id sent. Cannot retrieve tags.");
        }

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
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to update tags.");
        }
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
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to remove tags.");
        }
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
    @Transactional(readOnly = true)
    public Set<Cluster> getClustersForCommand(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to get clusters.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            return command.getClusters();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeTagForCommand(final String id, final String tag)
            throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No command id entered. Unable to remove tag.");
        }

        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.getTags().remove(tag);
            return command.getTags();
        } else {
            throw new GenieNotFoundException("No command with id " + id + " exists.");
        }
    }
}
