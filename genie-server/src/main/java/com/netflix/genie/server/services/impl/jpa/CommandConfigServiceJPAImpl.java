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

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Command_;
import com.netflix.genie.server.repository.ApplicationRepository;
import com.netflix.genie.server.repository.CommandRepository;
import com.netflix.genie.server.services.CommandConfigService;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

/**
 * Implementation of the CommandConfigService interface.
 *
 * @author amsharma
 * @author tgianos
 */
@Named
@Transactional(rollbackFor = CloudServiceException.class)
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
     * @param appRepo the application repository to use
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
     *
     * @throws CloudServiceException
     */
    @Override
    @Transactional(readOnly = true)
    public Command getCommand(final String id) throws CloudServiceException {
        LOG.debug("called");
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Id can't be null or empty.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            return command;
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
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
            final int page,
            final int limit) {
        LOG.debug("Called");

        //TODO: Switch to this in the CommandRepository
        final CriteriaBuilder cb = this.em.getCriteriaBuilder();
        final CriteriaQuery<Command> cq = cb.createQuery(Command.class);
        final Root<Command> c = cq.from(Command.class);
        final List<Predicate> predicates = new ArrayList<Predicate>();
        if (StringUtils.isNotEmpty(name)) {
            predicates.add(cb.equal(c.get(Command_.name), name));
        }
        if (StringUtils.isNotEmpty(userName)) {
            predicates.add(cb.equal(c.get(Command_.user), userName));
        }
        cq.where(cb.and(predicates.toArray(new Predicate[0])));
        final TypedQuery<Command> query = this.em.createQuery(cq);
        final int finalPage = page < 0 ? 0 : page;
        final int finalLimit = limit < 0 ? 1024 : limit;
        query.setMaxResults(finalLimit);
        query.setFirstResult(finalLimit * finalPage);
        return query.getResultList();
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Command createCommand(final Command command) throws CloudServiceException {
        Command.validate(command);
        LOG.debug("Called to create command " + command.toString());
        if (StringUtils.isEmpty(command.getId())) {
            command.setId(UUID.randomUUID().toString());
        }
        if (this.commandRepo.exists(command.getId())) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "A command with id " + command.getId() + " already exists");
        }
        final Set<Application> detachedApps = command.getApplications();
        final Set<Application> attachedApps = new HashSet<Application>();
        if (detachedApps != null) {
            for (final Application detached : detachedApps) {
                final Application app = this.appRepo.findOne(detached.getId());
                if (app != null) {
                    app.getCommands().add(command);
                    attachedApps.add(app);
                }
            }
        }
        command.setApplications(attachedApps);
        return this.commandRepo.save(command);
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Command updateCommand(
            final String id,
            final Command updateCommand) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered. Unable to update.");
        }
        if (StringUtils.isBlank(updateCommand.getId()) || !id.equals(updateCommand.getId())) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Command id either not entered or inconsistent with id passed in.");
        }
        LOG.debug("Called to update command with id " + id + " " + updateCommand.toString());
        final Command command = this.em.merge(updateCommand);
        Command.validate(command);
        return command;
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Command> deleteAllCommands() throws CloudServiceException {
        LOG.debug("Called to delete all commands");
        final Iterable<Command> commands = this.commandRepo.findAll();
        final List<Command> returnCommands = new ArrayList<Command>();
        for (final Command command : commands) {
            returnCommands.add(this.deleteCommand(command.getId()));
        }
        return returnCommands;
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Command deleteCommand(final String id) throws CloudServiceException {
        LOG.debug("Called to delete command config with id " + id);
        final Command command = this.commandRepo.findOne(id);
        if (command == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists to delete.");
        }
        //Remove the command from the associated Application references
        final Set<Application> apps = command.getApplications();
        if (apps != null) {
            for (final Application app : apps) {
                final Set<Command> commands = app.getCommands();
                if (commands != null) {
                    commands.remove(command);
                }
            }
        }
        this.commandRepo.delete(command);
        return command;
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> addConfigsForCommand(
            final String id,
            final Set<String> configs) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to add configurations.");
        }
        if (configs == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No configuration files entered.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.getConfigs().addAll(configs);
            return command.getConfigs();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForCommand(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to get configs.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            return command.getConfigs();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> updateConfigsForCommand(
            final String id,
            final Set<String> configs) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to update configurations.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.setConfigs(configs);
            return command.getConfigs();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> removeAllConfigsForCommand(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to remove jars.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            command.getConfigs().clear();
            return command.getConfigs();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> removeConfigForCommand(
            final String id,
            final String config) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to remove configuration.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            if (StringUtils.isNotBlank(config)) {
                command.getConfigs().remove(config);
            }
            return command.getConfigs();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<Application> addApplicationsForCommand(
            final String id,
            final Set<Application> applications) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to add applications.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            for (final Application detached : applications) {
                final Application app = this.appRepo.findOne(detached.getId());
                if (app != null) {
                    command.getApplications().add(app);
                    app.getCommands().add(command);
                }
            }
            return command.getApplications();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<Application> getApplicationsForCommand(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to get applications.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            return command.getApplications();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<Application> updateApplicationsForCommand(
            final String id,
            final Set<Application> applications) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to update applications.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            final Set<Application> apps = new HashSet<Application>();
            for (final Application detached : applications) {
                final Application app = this.appRepo.findOne(detached.getId());
                if (app != null) {
                    apps.add(app);
                    app.getCommands().add(command);
                }
            }
            command.setApplications(apps);
            return command.getApplications();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<Application> removeAllApplicationsForCommand(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to remove applications.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            for (final Application app : command.getApplications()) {
                app.getCommands().remove(command);
            }
            command.getApplications().clear();
            return command.getApplications();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<Application> removeApplicationForCommand(
            final String id,
            final String appId) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to remove application.");
        }
        if (StringUtils.isBlank(appId)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove application.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            final Application app = this.appRepo.findOne(appId);
            if (app != null) {
                app.getCommands().remove(command);
                command.getApplications().remove(app);
            }
            return command.getApplications();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<Cluster> getClustersForCommand(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to get clusters.");
        }
        final Command command = this.commandRepo.findOne(id);
        if (command != null) {
            return command.getClusters();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No command with id " + id + " exists.");
        }
    }
}
