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
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.services.CommandConfigService;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the CommandConfigService interface.
 *
 * @author amsharma
 * @author tgianos
 */
public class CommandConfigServiceJPAImpl implements CommandConfigService {

    private static final Logger LOG = LoggerFactory
            .getLogger(CommandConfigServiceJPAImpl.class);

    private final PersistenceManager<Command> pm;

    /**
     * Default constructor.
     */
    public CommandConfigServiceJPAImpl() {
        // instantiate PersistenceManager
        this.pm = new PersistenceManager<Command>();
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Command getCommand(final String id) throws CloudServiceException {
        LOG.debug("called");
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Id can't be null or empty.");
        }
        final EntityManager em = this.pm.createEntityManager();
        try {
            //No need for a transation for a get
            final Command command = em.find(Command.class, id);
            if (command != null) {
                return command;
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Command> getCommands(
            final String name,
            final String userName,
            final int page,
            final int limit) {
        LOG.debug("Called");

        final EntityManager em = this.pm.createEntityManager();
        try {
            final CriteriaBuilder cb = em.getCriteriaBuilder();
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
            final TypedQuery<Command> query = em.createQuery(cq);
            final int finalPage = page < 0 ? PersistenceManager.DEFAULT_PAGE_NUMBER : page;
            final int finalLimit = limit < 0 ? PersistenceManager.DEFAULT_PAGE_SIZE : limit;
            query.setMaxResults(finalLimit);
            query.setFirstResult(finalLimit * finalPage);
            return query.getResultList();
        } finally {
            em.close();
        }
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
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            if (StringUtils.isEmpty(command.getId())) {
                command.setId(UUID.randomUUID().toString());
            }
            if (em.contains(command)) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "A command with id " + command.getId() + " already exists");
            }
            final List<Application> detachedApps = command.getApplications();
            final List<Application> attachedApps = new ArrayList<Application>();
            if (detachedApps != null) {
                for (final Application detached : detachedApps) {
                    final Application app = em.find(Application.class, detached.getId());
                    if (app != null) {
                        app.getCommands().add(command);
                        attachedApps.add(app);
                    }
                }
            }
            command.setApplications(attachedApps);
            em.persist(command);
            trans.commit();
            return command;
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
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
        final EntityManager em = pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.merge(updateCommand);
            Command.validate(command);
            trans.commit();
            return command;
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Command> deleteAllCommands() throws CloudServiceException {
        LOG.debug("Called to delete all commands");
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final List<Command> commands = this.getCommands(null, null, 0, Integer.MAX_VALUE);
            for (final Command command : commands) {
                this.deleteCommand(command.getId());
            }
            trans.commit();
            return commands;
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Command deleteCommand(final String id) throws CloudServiceException {
        LOG.debug("Called to delete command config with id " + id);
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.find(Command.class, id);
            if (command == null) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists to delete.");
            }
            //Remove the command from the associated Application references
            final List<Application> apps = command.getApplications();
            if (apps != null) {
                for (final Application app : apps) {
                    final Set<Command> commands = app.getCommands();
                    if (commands != null) {
                        commands.remove(command);
                    }
                }
            }
            em.remove(command);
            trans.commit();
            return command;
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
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
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.find(Command.class, id);
            if (command != null) {
                command.getConfigs().addAll(configs);
                trans.commit();
                return command.getConfigs();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> getConfigsForCommand(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to get configs.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final Command command = em.find(Command.class, id);
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
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.find(Command.class, id);
            if (command != null) {
                command.setConfigs(configs);
                trans.commit();
                return command.getConfigs();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
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
                    "No command id entered. Unable to remove configs.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.find(Command.class, id);
            if (command != null) {
                command.getConfigs().clear();
                trans.commit();
                return command.getConfigs();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
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
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.find(Command.class, id);
            if (command != null) {
                if (StringUtils.isNotBlank(config)) {
                    command.getConfigs().remove(config);
                }
                trans.commit();
                return command.getConfigs();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Application> addApplicationsForCommand(
            final String id,
            final List<Application> applications) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to add applications.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.find(Command.class, id);
            if (command != null) {
                for (final Application detached : applications) {
                    final Application app
                            = em.find(Application.class, detached.getId());
                    if (app != null) {
                        command.getApplications().add(app);
                        app.getCommands().add(command);
                    } else {
                        throw new CloudServiceException(
                                HttpURLConnection.HTTP_NOT_FOUND,
                                "No application with id " + detached.getId() + " exists.");
                    }
                }
                trans.commit();
                return command.getApplications();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Application> getApplicationsForCommand(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to get applications.");
        }
        final EntityManager em = this.pm.createEntityManager();
        try {
            final Command command = em.find(Command.class, id);
            if (command != null) {
                return command.getApplications();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Application> updateApplicationsForCommand(
            final String id,
            final List<Application> applications) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to update applications.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.find(Command.class, id);
            if (command != null) {
                final List<Application> apps = new ArrayList<Application>();
                for (final Application detached : applications) {
                    final Application app
                            = em.find(Application.class, detached.getId());
                    if (app != null) {
                        apps.add(app);
                        app.getCommands().add(command);
                    } else { 
                        throw new CloudServiceException(
                                HttpURLConnection.HTTP_NOT_FOUND,
                                "No application with id " + detached.getId() + " exists.");
                    }
                }
                command.setApplications(apps);
                trans.commit();
                return command.getApplications();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Application> removeAllApplicationsForCommand(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to remove applications.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.find(Command.class, id);
            if (command != null) {
                for (final Application app : command.getApplications()) {
                    app.getCommands().remove(command);
                }
                command.getApplications().clear();
                trans.commit();
                return command.getApplications();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Application> removeApplicationForCommand(
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
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.find(Command.class, id);
            if (command != null) {
                final Application app = em.find(Application.class, appId);
                if (app != null) {
                    app.getCommands().remove(command);
                    command.getApplications().remove(app);
                }
                trans.commit();
                return command.getApplications();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<Cluster> getClustersForCommand(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to get clusters.");
        }
        final EntityManager em = this.pm.createEntityManager();
        try {
            final Command command = em.find(Command.class, id);
            if (command != null) {
                return command.getClusters();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists.");
            }
        } finally {
            em.close();
        }
    }
}
