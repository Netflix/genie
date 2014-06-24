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
import com.netflix.genie.server.repository.ApplicationRepository;
import com.netflix.genie.server.services.ApplicationConfigService;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
 * OpenJPA based implementation of the ApplicationConfigService.
 *
 * @author amsharma
 * @author tgianos
 */
@Named
@Transactional(rollbackFor = CloudServiceException.class)
public class ApplicationConfigServiceJPAImpl implements ApplicationConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationConfigServiceJPAImpl.class);

    @PersistenceContext
    private EntityManager em;
    
    private final ApplicationRepository applicationRepo;

    /**
     * Default constructor.
     * 
     * @param applicationRepo The application repository to use
     */
    @Inject
    public ApplicationConfigServiceJPAImpl(final ApplicationRepository applicationRepo) {
        this.applicationRepo = applicationRepo;
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    @Transactional(readOnly = true)
    public Application getApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            //Messages will be logged by exception mapper at resource level
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered. Unable to get");
        }
        LOG.debug("Called with id " + id);
        final Application app = this.applicationRepo.findOne(id);
        if (app == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id);
        } else {
            return app;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Application> getApplications(
            final String name,
            final String userName,
            final int page,
            final int limit) {
        //TODO: Switch to application repo usage
        LOG.debug("Called");
        final CriteriaBuilder cb = this.em.getCriteriaBuilder();
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
        final TypedQuery<Application> query = this.em.createQuery(cq);
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
    public Application createApplication(
            final Application app) throws CloudServiceException {
        Application.validate(app);
        LOG.debug("Called with application: " + app.toString());
        if (app.getId() != null && this.applicationRepo.exists(app.getId())) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_CONFLICT,
                    "An application with id " + app.getId() + " already exists");
        }
        return this.applicationRepo.save(app);
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
        final Application app = this.em.merge(updateApp);
        Application.validate(app);
        return app;
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Application> deleteAllApplications() throws CloudServiceException {
        LOG.debug("Called");
        final Iterable<Application> apps = this.applicationRepo.findAll();
        final List<Application> returnApps = new ArrayList<Application>();
        for (final Application app : apps) {
            returnApps.add(this.deleteApplication(app.getId()));
        }
        return returnApps;
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
        final Application app = this.applicationRepo.findOne(id);
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
        this.applicationRepo.delete(app);
        return app;
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
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getConfigs().addAll(configs);
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
    public Set<String> updateConfigsForApplication(
            final String id,
            final Set<String> configs) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to update configurations.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.setConfigs(configs);
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
    @Transactional(readOnly = true)
    public Set<String> getConfigsForApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to get configs.");
        }
        final Application app = this.applicationRepo.findOne(id);
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
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getConfigs().clear();
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
    public Set<String> removeApplicationConfig(
            final String id,
            final String config) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove configuration.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            if (StringUtils.isNotBlank(config)) {
                app.getConfigs().remove(config);
            }
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
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getJars().addAll(jars);
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
    @Transactional(readOnly = true)
    //TODO: Code is repetetive with configs. Refactor for reuse
    public Set<String> getJarsForApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to get jars.");
        }
        final Application app = this.applicationRepo.findOne(id);
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
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.setJars(jars);
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
    public Set<String> removeAllJarsForApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove jars.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getJars().clear();
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
    public Set<String> removeJarForApplication(
            final String id,
            final String jar) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove jar.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            if (StringUtils.isNotBlank(jar)) {
                app.getJars().remove(jar);
            }
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
    @Transactional(readOnly = true)
    public Set<Command> getCommandsForApplication(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to get commands.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            return app.getCommands();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }
}
