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
import com.netflix.genie.common.model.*;
import com.netflix.genie.server.repository.jpa.ApplicationRepository;
import com.netflix.genie.server.repository.jpa.ApplicationSpecs;
import com.netflix.genie.server.repository.jpa.CommandRepository;
import com.netflix.genie.server.repository.jpa.CommandSpecs;
import com.netflix.genie.server.services.ApplicationConfigService;

import java.util.*;
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
 * OpenJPA based implementation of the ApplicationConfigService.
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
public class ApplicationConfigServiceJPAImpl implements ApplicationConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationConfigServiceJPAImpl.class);
    private final ApplicationRepository applicationRepo;
    private final CommandRepository commandRepo;
    @PersistenceContext
    private EntityManager em;

    /**
     * Default constructor.
     *
     * @param applicationRepo The application repository to use
     */
    public ApplicationConfigServiceJPAImpl(final ApplicationRepository applicationRepo,
                                           final CommandRepository commandRepo) {
        this.applicationRepo = applicationRepo;
        this.commandRepo = commandRepo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Application createApplication(
            @NotNull(message = "No application entered to create.")
            @Valid
            final Application app) throws GenieException {
        LOG.debug("Called with application: " + app.toString());
        if (app.getId() != null && this.applicationRepo.exists(app.getId())) {
            throw new GenieConflictException("An application with id " + app.getId() + " already exists");
        }
        return this.applicationRepo.save(app);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Application getApplication(
            @NotBlank(message = "No id entered. Unable to get")
            final String id
    ) throws GenieException {
        LOG.debug("Called with id " + id);
        final Application app = this.applicationRepo.findOne(id);
        if (app == null) {
            throw new GenieNotFoundException("No application with id " + id);
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
            final Set<ApplicationStatus> statuses,
            final Set<String> tags,
            final int page,
            final int limit,
            final boolean descending,
            final Set<String> orderBys) {
        LOG.debug("Called");

        final PageRequest pageRequest = JPAUtils.getPageRequest(
                page, limit, descending, orderBys, Application_.class, Application_.updated.getName()
        );

        @SuppressWarnings("unchecked")
        final List<Application> apps = this.applicationRepo.findAll(
                ApplicationSpecs.find(name, userName, statuses, tags),
                pageRequest).getContent();
        return apps;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Application updateApplication(
            @NotBlank(message = "No application id entered. Unable to update.")
            final String id,
            @NotNull(message = "No application information entered. Unable to update.")
            final Application updateApp
    ) throws GenieException {
        if (!this.applicationRepo.exists(id)) {
            throw new GenieNotFoundException("No application information entered. Unable to update.");
        }
        if (StringUtils.isNotBlank(updateApp.getId())
                && !id.equals(updateApp.getId())) {
            throw new GenieBadRequestException("Application id either not entered or inconsistent with id passed in.");
        }

        //Set the id if it's not set so we can merge
        if (StringUtils.isBlank(updateApp.getId())) {
            updateApp.setId(id);
        }
        LOG.debug("Called with app " + updateApp.toString());
        return this.em.merge(updateApp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Application> deleteAllApplications() throws GenieException {
        LOG.debug("Called");
        final Iterable<Application> apps = this.applicationRepo.findAll();
        final List<Application> returnApps = new ArrayList<>();
        for (final Application app : apps) {
            returnApps.add(this.deleteApplication(app.getId()));
        }
        return returnApps;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Application deleteApplication(
            @NotBlank(message = "No application id entered. Unable to delete.")
            final String id
    ) throws GenieException {
        LOG.debug("Called with id " + id);
        final Application app = this.applicationRepo.findOne(id);
        if (app == null) {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }

        if (app.getCommands() != null) {
            final Set<Command> commands = new HashSet<>();
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
     */
    @Override
    public Set<String> addConfigsToApplication(
            @NotBlank(message = "No application id entered. Unable to add configurations.")
            final String id,
            @NotEmpty(message = "No configuration files entered.")
            final Set<String> configs
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getConfigs().addAll(configs);
            return app.getConfigs();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForApplication(
            @NotBlank(message = "No application id entered. Unable to get configs.")
            final String id
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            return app.getConfigs();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> updateConfigsForApplication(
            @NotBlank(message = "No application id entered. Unable to update configurations.")
            final String id,
            @NotNull(message = "No configs entered. Unable to update. If you want, use delete API.")
            final Set<String> configs
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.setConfigs(configs);
            return app.getConfigs();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeAllConfigsForApplication(
            @NotBlank(message = "No application id entered. Unable to remove configs.")
            final String id
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getConfigs().clear();
            return app.getConfigs();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeConfigForApplication(
            @NotBlank(message = "No application id entered. Unable to remove configuration.")
            final String id,
            @NotBlank(message = "No config entered. Unable to remove.")
            final String config
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getConfigs().remove(config);
            return app.getConfigs();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> addJarsForApplication(
            @NotBlank(message = "No application id entered. Unable to add jars.")
            final String id,
            @NotEmpty(message = "No jars entered. Unable to add jars.")
            final Set<String> jars
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getJars().addAll(jars);
            return app.getJars();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    //TODO: Code is repetetive with configs. Refactor for reuse
    public Set<String> getJarsForApplication(
            @NotBlank(message = "No application id entered. Unable to get jars.")
            final String id
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            return app.getJars();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> updateJarsForApplication(
            @NotBlank(message = "No application id entered. Unable to update jars.")
            final String id,
            @NotNull(message = "No jars entered. Unable to update.")
            final Set<String> jars
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.setJars(jars);
            return app.getJars();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeAllJarsForApplication(
            @NotBlank(message = "No application id entered. Unable to remove jars.")
            final String id
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getJars().clear();
            return app.getJars();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeJarForApplication(
            @NotBlank(message = "No application id entered. Unable to remove jar.")
            final String id,
            @NotBlank(message = "No jar entiered. Unable to remove jar.")
            final String jar
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            app.getJars().remove(jar);
            return app.getJars();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> addTagsForApplication(
            @NotBlank(message = "No application id entered. Unable to add tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to add.")
            final Set<String> tags
    ) throws GenieException {
        final Application application = this.applicationRepo.findOne(id);
        if (application != null) {
            application.getTags().addAll(tags);
            return application.getTags();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForApplication(
            @NotBlank(message = "No application id entered. Cannot retrieve tags.")
            final String id
    ) throws GenieException {
        final Application application = this.applicationRepo.findOne(id);
        if (application != null) {
            return application.getTags();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> updateTagsForApplication(
            @NotBlank(message = "No application id entered. Unable to update tags.")
            final String id,
            @NotNull(message = "No tags entered unable to update tags.")
            final Set<String> tags
    ) throws GenieException {
        final Application application = this.applicationRepo.findOne(id);
        if (application != null) {
            application.setTags(tags);
            return application.getTags();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeAllTagsForApplication(
            @NotBlank(message = "No application id entered. Unable to remove tags.")
            final String id
    ) throws GenieException {
        final Application application = this.applicationRepo.findOne(id);
        if (application != null) {
            application.getTags().clear();
            return application.getTags();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeTagForApplication(
            @NotBlank(message = "No application id entered. Unable to remove tag.")
            final String id,
            @NotBlank(message = "No tag entered. Unable to remove.")
            final String tag
    ) throws GenieException {
        final Application application = this.applicationRepo.findOne(id);
        if (application != null) {
            application.getTags().remove(tag);
            return application.getTags();
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Command> getCommandsForApplication(
            @NotBlank(message = "No application id entered. Unable to get commands.")
            final String id,
            final Set<CommandStatus> statuses
    ) throws GenieException {
        final Application app = this.applicationRepo.findOne(id);
        if (app != null) {
            @SuppressWarnings("unchecked")
            final List<Command> commands = this.commandRepo.findAll(
                    CommandSpecs.findCommandsForApplication(
                            id,
                            statuses
                    ));
            return commands;
        } else {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
    }
}
