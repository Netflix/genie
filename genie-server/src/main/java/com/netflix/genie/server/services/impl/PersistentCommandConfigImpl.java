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
package com.netflix.genie.server.services.impl;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Application;
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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the CommandConfigService interface.
 *
 * @author amsharma
 * @author tgianos
 */
public class PersistentCommandConfigImpl implements CommandConfigService {

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistentCommandConfigImpl.class);

    private final PersistenceManager<Command> pm;

    /**
     * Default constructor.
     */
    public PersistentCommandConfigImpl() {
        // instantiate PersistenceManager
        this.pm = new PersistenceManager<Command>();
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Command getCommandConfig(final String id) throws CloudServiceException {
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
    public List<Command> getCommandConfigs(
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
    public Command createCommandConfig(final Command command) throws CloudServiceException {
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
            final Set<Application> detachedApps = command.getApplications();
            final Set<Application> attachedApps = new HashSet<Application>();
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
    public Command updateCommandConfig(
            final String id,
            final Command updateCommand) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered. Unable to update.");
        }
        if (updateCommand == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command info entered. Unable to update.");
        }
        LOG.debug("Called to update command with id " + id + " " + updateCommand.toString());
        final EntityManager em = pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Command command = em.find(Command.class, id);
            if (command == null) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + id + " exists to update.");
            }
            if (StringUtils.isNotEmpty(updateCommand.getName())) {
                command.setName(updateCommand.getName());
            }
            if (StringUtils.isNotEmpty(updateCommand.getEnvPropFile())) {
                command.setEnvPropFile(updateCommand.getEnvPropFile());
            }
            if (StringUtils.isNotEmpty(updateCommand.getExecutable())) {
                command.setExecutable(updateCommand.getExecutable());
            }
            if (StringUtils.isNotEmpty(updateCommand.getJobType())) {
                command.setJobType(updateCommand.getJobType());
            }
            if (StringUtils.isNotEmpty(updateCommand.getUser())) {
                command.setUser(updateCommand.getUser());
            }
            if (StringUtils.isNotEmpty(updateCommand.getVersion())) {
                command.setVersion(updateCommand.getVersion());
            }
            if (updateCommand.getStatus() != null
                    && updateCommand.getStatus() != command.getStatus()) {
                command.setStatus(updateCommand.getStatus());
            }
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
    public Command deleteCommandConfig(final String id) throws CloudServiceException {
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
            final Set<Application> apps = command.getApplications();
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
}
