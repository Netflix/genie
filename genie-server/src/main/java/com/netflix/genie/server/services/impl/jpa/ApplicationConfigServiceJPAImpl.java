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
import com.netflix.genie.common.model.Application_;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.services.ApplicationConfigService;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
 * OpenJPA based implementation of the ApplicationConfigService.
 *
 * @author amsharma
 * @author tgianos
 */
public class ApplicationConfigServiceJPAImpl implements ApplicationConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationConfigServiceJPAImpl.class);

    private final PersistenceManager<Application> pm;

    /**
     * Default constructor.
     */
    public ApplicationConfigServiceJPAImpl() {
        // instantiate PersistenceManager
        this.pm = new PersistenceManager<Application>();
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Application getApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            //Messages will be logged by exception mapper at resource level
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered. Unable to get");
        }
        LOG.debug("Called with id " + id);
        final EntityManager em = this.pm.createEntityManager();
        try {
            final Application app = em.find(Application.class, id);
            if (app == null) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id);
            } else {
                return app;
            }
        } finally {
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Application> getApplications(
            final String name,
            final String userName,
            final int page,
            final int limit) {
        LOG.debug("Called");

        final EntityManager em = this.pm.createEntityManager();
        try {
            final CriteriaBuilder cb = em.getCriteriaBuilder();
            final CriteriaQuery<Application> cq = cb.createQuery(Application.class);
            final Root<Application> a = cq.from(Application.class);
            final List<Predicate> predicates = new ArrayList<Predicate>();
            if (StringUtils.isNotEmpty(name)) {
                predicates.add(cb.equal(a.get(Application_.name), name));
            }
            if (StringUtils.isNotEmpty(userName)) {
                predicates.add(cb.equal(a.get(Application_.user), userName));
            }
            cq.where(predicates.toArray(new Predicate[0]));
            final TypedQuery<Application> query = em.createQuery(cq);
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
    public Application createApplication(
            final Application app) throws CloudServiceException {
        Application.validate(app);
        LOG.debug("Called with application: " + app.toString());
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            if (em.contains(app)) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_CONFLICT,
                        "An application with id " + app.getId() + " already exists");
            }
            em.persist(app);
            trans.commit();
            return app;
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
    public Application updateApplication(
            final String id,
            final Application updateApp) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to update.");
        }
        if (updateApp == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application information entered. Unable to update.");
        }
        if (StringUtils.isBlank(updateApp.getId()) || !id.equals(updateApp.getId())) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Application id either not entered or inconsistent with id passed in.");
        }
        LOG.debug("Called with app " + updateApp.toString());
        //TODO: What to do about sets like jars, etc?
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.merge(updateApp);
            Application.validate(app);
            trans.commit();
            return app;
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
    public List<Application> deleteAllApplications() throws CloudServiceException {
        LOG.debug("Called");
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final List<Application> apps = this.getApplications(null, null, 0, Integer.MAX_VALUE);
            for (final Application app : apps) {
                this.deleteApplication(app.getId());
            }
            trans.commit();
            return apps;
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
    public Application deleteApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to delete.");
        }
        LOG.debug("Called with id " + id);
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.find(Application.class, id);
            if (app == null) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
            }
            final Set<Command> commands = app.getCommands();
            if (commands != null) {
                for (final Command command : commands) {
                    final Set<Application> apps = command.getApplications();
                    if (apps != null) {
                        apps.remove(app);
                    }
                }
            }
            em.remove(app);
            trans.commit();
            return app;
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
    public Set<String> addConfigsToApplication(
            final String id,
            final Set<String> configs) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to add configurations.");
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
            final Application app = em.find(Application.class, id);
            if (app != null) {
                app.getConfigs().addAll(configs);
                trans.commit();
                return app.getConfigs();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
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
    public Set<String> updateConfigsForApplication(
            final String id,
            final Set<String> configs) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to update configurations.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.find(Application.class, id);
            if (app != null) {
                app.setConfigs(configs);
                trans.commit();
                return app.getConfigs();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
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
    public Set<String> getConfigsForApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to get configs.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final Application app = em.find(Application.class, id);
        if (app != null) {
            return app.getConfigs();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> removeAllConfigsForApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove jars.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.find(Application.class, id);
            if (app != null) {
                app.getConfigs().clear();
                trans.commit();
                return app.getConfigs();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
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
    public Set<String> removeApplicationConfig(
            final String id,
            final String config) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove configuration.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.find(Application.class, id);
            if (app != null) {
                if (StringUtils.isNotBlank(config)) {
                    app.getConfigs().remove(config);
                }
                trans.commit();
                return app.getConfigs();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
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
    public Set<String> addJarsForApplication(
            final String id,
            final Set<String> jars) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to add jar.");
        }
        if (jars == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No jar entered. Unable to add jars.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.find(Application.class, id);
            if (app != null) {
                app.getJars().addAll(jars);
                trans.commit();
                return app.getJars();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
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
    //TODO: Code is repetetive with configs. Refactor for reuse
    public Set<String> getJarsForApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to get jars.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final Application app = em.find(Application.class, id);
        if (app != null) {
            return app.getJars();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> updateJarsForApplication(
            final String id,
            final Set<String> jars) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to update jars.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.find(Application.class, id);
            if (app != null) {
                app.setJars(jars);
                trans.commit();
                return app.getJars();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
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
    public Set<String> removeAllJarsForApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove jars.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.find(Application.class, id);
            if (app != null) {
                app.getJars().clear();
                trans.commit();
                return app.getJars();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
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
    public Set<String> removeJarForApplication(
            final String id,
            final String jar) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove jar.");
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Application app = em.find(Application.class, id);
            if (app != null) {
                if (StringUtils.isNotBlank(jar)) {
                    app.getJars().remove(jar);
                }
                trans.commit();
                return app.getJars();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
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
    public Set<Command> getCommandsForApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to get commands.");
        }
        final EntityManager em = this.pm.createEntityManager();
        try {
            final Application app = em.find(Application.class, id);
            if (app != null) {
                return app.getCommands();
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists.");
            }
        } finally {
            em.close();
        }
    }
}
