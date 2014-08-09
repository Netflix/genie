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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Application_;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.server.repository.jpa.ApplicationRepository;
import com.netflix.genie.server.repository.jpa.ApplicationSpecs;
import com.netflix.genie.server.services.ApplicationConfigService;
import org.springframework.data.domain.Sort.Direction;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * OpenJPA based implementation of the ApplicationConfigService.
 *
 * @author amsharma
 * @author tgianos
 */
@Named
@Transactional(rollbackFor = GenieException.class)
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
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public Application getApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            //Messages will be logged by exception mapper at resource level
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered. Unable to get");
        }
        LOG.debug("Called with id " + id);
        final Application app = this.applicationRepo.findOne(id);
        if (app == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id);
        }

        return app;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Application> getApplications(
            final String name,
            final String userName,
            final Set<String> tags,
            final int page,
            final int limit) {
        LOG.debug("Called");

        final PageRequest pageRequest = new PageRequest(
                page < 0 ? 0 : page,
                limit < 0 ? 1024 : limit,
                Direction.DESC,
                Application_.updated.getName()
        );

        @SuppressWarnings("unchecked")
        final List<Application> apps = this.applicationRepo.findAll(
                ApplicationSpecs.findByNameAndUserAndTags(name, userName, tags),
                pageRequest).getContent();
        return apps;
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Application createApplication(
            final Application app) throws GenieException {
        if (app == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application entered to create.");
        }
        app.validate();

        LOG.debug("Called with application: " + app.toString());
        if (app.getId() != null && this.applicationRepo.exists(app.getId())) {
            throw new GenieException(
                    HttpURLConnection.HTTP_CONFLICT,
                    "An application with id " + app.getId() + " already exists");
        }
        return this.applicationRepo.save(app);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Application updateApplication(
            final String id,
            final Application updateApp) throws GenieException {
        if (StringUtils.isEmpty(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to update.");
        }
        if (updateApp == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application information entered. Unable to update.");
        }
        if (StringUtils.isBlank(updateApp.getId()) || !id.equals(updateApp.getId())) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Application id either not entered or inconsistent with id passed in.");
        }
        LOG.debug("Called with app " + updateApp.toString());
        final Application app = this.em.merge(updateApp);
        app.validate();
        return app;
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public List<Application> deleteAllApplications() throws GenieException {
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
     * @throws GenieException
     */
    @Override
    public Application deleteApplication(
            final String id) throws GenieException {
        if (StringUtils.isEmpty(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to delete.");
        }
        LOG.debug("Called with id " + id);
        final Application app = this.applicationRepo.findOne(id);
        if (app == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }

        if (app.getCommands() != null) {
            final Set<Command> commands = new HashSet<Command>();
            commands.addAll(app.getCommands());
            for (final Command command : commands) {
                command.setApplication(null);
            }
        }
        this.applicationRepo.delete(app);
        return app;
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> addConfigsToApplication(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to add configurations.");
        }
        if (configs == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No configuration files entered.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getConfigs().addAll(configs);
            return app.getConfigs();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> updateConfigsForApplication(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to update configurations.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.setConfigs(configs);
            return app.getConfigs();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to get configs.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            return app.getConfigs();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> removeAllConfigsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove jars.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getConfigs().clear();
            return app.getConfigs();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> removeApplicationConfig(
            final String id,
            final String config) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
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
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> addJarsForApplication(
            final String id,
            final Set<String> jars) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to add jar.");
        }
        if (jars == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No jar entered. Unable to add jars.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getJars().addAll(jars);
            return app.getJars();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    //TODO: Code is repetetive with configs. Refactor for reuse
    public Set<String> getJarsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to get jars.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            return app.getJars();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> updateJarsForApplication(
            final String id,
            final Set<String> jars) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to update jars.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.setJars(jars);
            return app.getJars();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> removeAllJarsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove jars.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getJars().clear();
            return app.getJars();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> removeJarForApplication(
            final String id,
            final String jar) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
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
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> addTagsForApplication(
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to add tags.");
        }
        if (tags == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No tags entered.");
        }
        final Application application = this.applicationRepo.findOne(id);
        if (application != null) {
            application.getTags().addAll(tags);
            return application.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForApplication(
            final String id)
            throws GenieException {

        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id sent. Cannot retrieve tags.");
        }

        final Application application = this.applicationRepo.findOne(id);
        if (application != null) {
            return application.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> updateTagsForApplication(
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to update tags.");
        }
        final Application application = this.applicationRepo.findOne(id);
        if (application != null) {
            application.setTags(tags);
            return application.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> removeAllTagsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove tags.");
        }
        final Application application = this.applicationRepo.findOne(id);
        if (application != null) {
            application.getTags().clear();
            return application.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<Command> getCommandsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to get commands.");
        }
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            return app.getCommands();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }

    @Override
    public Set<String> removeTagForApplication(String id, String tag)
            throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application id entered. Unable to remove tag.");
        }
        if (StringUtils.isBlank(tag)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No tag entered. Unable to remove tag.");
        }
        if (tag.equals(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Cannot delete application id from the tags list.");
        }

        final Application application = this.applicationRepo.findOne(id);
        if (application != null) {
            application.getTags().remove(tag);
            return application.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No application with id " + id + " exists.");
        }
    }
}
