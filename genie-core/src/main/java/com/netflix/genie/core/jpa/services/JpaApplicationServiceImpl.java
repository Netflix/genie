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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.jpa.entities.ApplicationEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.specifications.JpaApplicationSpecs;
import com.netflix.genie.core.jpa.specifications.JpaCommandSpecs;
import com.netflix.genie.core.services.ApplicationService;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
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
 * JPA based implementation of the ApplicationService.
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
public class JpaApplicationServiceImpl implements ApplicationService {

    private static final Logger LOG = LoggerFactory.getLogger(JpaApplicationServiceImpl.class);
    private final JpaApplicationRepository applicationRepo;
    private final JpaCommandRepository commandRepo;

    /**
     * Default constructor.
     *
     * @param applicationRepo The application repository to use
     * @param commandRepo     The command repository to use
     */
    @Autowired
    public JpaApplicationServiceImpl(
        final JpaApplicationRepository applicationRepo,
        final JpaCommandRepository commandRepo
    ) {
        this.applicationRepo = applicationRepo;
        this.commandRepo = commandRepo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createApplication(
        @NotNull(message = "No application entered to create.")
        @Valid
        final Application app
    ) throws GenieException {
        LOG.debug("Called with application: {}", app.toString());
        if (app.getId() != null && this.applicationRepo.exists(app.getId())) {
            throw new GenieConflictException("An application with id " + app.getId() + " already exists");
        }

        final ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setId(app.getId());
        applicationEntity.setName(app.getName());
        applicationEntity.setUser(app.getUser());
        applicationEntity.setVersion(app.getVersion());
        applicationEntity.setDescription(app.getDescription());
        applicationEntity.setStatus(app.getStatus());
        applicationEntity.setSetupFile(app.getSetupFile());
        applicationEntity.setApplicationTags(app.getTags());
        applicationEntity.setConfigs(app.getConfigs());
        applicationEntity.setDependencies(app.getDependencies());

        if (StringUtils.isBlank(applicationEntity.getId())) {
            applicationEntity.setId(UUID.randomUUID().toString());
        }

        final String id = applicationEntity.getId();
        this.applicationRepo.save(applicationEntity);
        return id;
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
        LOG.debug("Called with id {}", id);
        return this.findApplication(id).getDTO();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Application> getApplications(
        final String name,
        final String userName,
        final Set<ApplicationStatus> statuses,
        final Set<String> tags,
        final Pageable page
    ) {
        LOG.debug("Called");

        @SuppressWarnings("unchecked")
        final Page<ApplicationEntity> applicationEntities
            = this.applicationRepo.findAll(JpaApplicationSpecs.find(name, userName, statuses, tags), page);

        return applicationEntities.map(ApplicationEntity::getDTO);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateApplication(
        @NotBlank(message = "No application id entered. Unable to update.")
        final String id,
        @NotNull(message = "No application information entered. Unable to update.")
        @Valid
        final Application updateApp
    ) throws GenieException {
        if (!this.applicationRepo.exists(id)) {
            throw new GenieNotFoundException("No application information entered. Unable to update.");
        }
        if (!id.equals(updateApp.getId())) {
            throw new GenieBadRequestException("Application id inconsistent with id passed in.");
        }

        LOG.debug("Called with app {}", updateApp.toString());
        final ApplicationEntity applicationEntity = this.findApplication(id);
        applicationEntity.setName(updateApp.getName());
        applicationEntity.setUser(updateApp.getUser());
        applicationEntity.setVersion(updateApp.getVersion());
        applicationEntity.setDescription(updateApp.getDescription());
        applicationEntity.setStatus(updateApp.getStatus());
        applicationEntity.setSetupFile(updateApp.getSetupFile());
        applicationEntity.setConfigs(updateApp.getConfigs());
        applicationEntity.setDependencies(updateApp.getDependencies());
        applicationEntity.setApplicationTags(updateApp.getTags());

        this.applicationRepo.save(applicationEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllApplications() throws GenieException {
        LOG.debug("Called");
        // Check to make sure the application isn't tied to any existing commands
        for (final ApplicationEntity applicationEntity : this.applicationRepo.findAll()) {
            final Set<CommandEntity> commandEntities = applicationEntity.getCommands();
            if (commandEntities != null && !commandEntities.isEmpty()) {
                throw new GeniePreconditionException(
                    "Unable to delete app " + applicationEntity.getId() + " as it is attached to "
                        + commandEntities.size() + " commands still."
                );
            }
        }
        this.applicationRepo.deleteAll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteApplication(
        @NotBlank(message = "No application id entered. Unable to delete.")
        final String id
    ) throws GenieException {
        LOG.debug("Called with id {}", id);
        final ApplicationEntity applicationEntity = this.findApplication(id);
        final Set<CommandEntity> commandEntities = applicationEntity.getCommands();
        if (commandEntities != null && !commandEntities.isEmpty()) {
            throw new GeniePreconditionException(
                "Unable to delete app " + applicationEntity.getId() + " as it is attached to "
                    + commandEntities.size() + " commands."
            );
        }

        this.applicationRepo.delete(applicationEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addConfigsToApplication(
        @NotBlank(message = "No application id entered. Unable to add configurations.")
        final String id,
        @NotEmpty(message = "No configuration files entered.")
        final Set<String> configs
    ) throws GenieException {
        this.findApplication(id).getConfigs().addAll(configs);
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
        return this.findApplication(id).getConfigs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to update configurations.")
        final String id,
        @NotNull(message = "No configs entered. Unable to update. If you want, use delete API.")
        final Set<String> configs
    ) throws GenieException {
        this.findApplication(id).setConfigs(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to remove configs.")
        final String id
    ) throws GenieException {
        this.findApplication(id).getConfigs().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeConfigForApplication(
        @NotBlank(message = "No application id entered. Unable to remove configuration.")
        final String id,
        @NotBlank(message = "No config entered. Unable to remove.")
        final String config
    ) throws GenieException {
        this.findApplication(id).getConfigs().remove(config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to add dependencies.")
        final String id,
        @NotEmpty(message = "No dependencies entered. Unable to add dependencies.")
        final Set<String> dependencies
    ) throws GenieException {
        this.findApplication(id).getDependencies().addAll(dependencies);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to get dependencies.")
        final String id
    ) throws GenieException {
        return this.findApplication(id).getDependencies();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to update dependencies.")
        final String id,
        @NotNull(message = "No dependencies entered. Unable to update.")
        final Set<String> dependencies
    ) throws GenieException {
        this.findApplication(id).setDependencies(dependencies);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to remove dependencies.")
        final String id
    ) throws GenieException {
        this.findApplication(id).getDependencies().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDependencyForApplication(
        @NotBlank(message = "No application id entered. Unable to remove dependency.")
        final String id,
        @NotBlank(message = "No dependency entered. Unable to remove dependency.")
        final String dependency
    ) throws GenieException {
        this.findApplication(id).getDependencies().remove(dependency);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to add tags.")
        final String id,
        @NotEmpty(message = "No tags entered. Unable to add.")
        final Set<String> tags
    ) throws GenieException {
        final ApplicationEntity app = this.findApplication(id);
        final Set<String> appTags = app.getApplicationTags();
        appTags.addAll(tags);
        app.setApplicationTags(appTags);
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
        return this.findApplication(id).getApplicationTags();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to update tags.")
        final String id,
        @NotNull(message = "No tags entered unable to update tags.")
        final Set<String> tags
    ) throws GenieException {
        this.findApplication(id).setApplicationTags(tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to remove tags.")
        final String id
    ) throws GenieException {
        this.findApplication(id).setApplicationTags(Sets.newHashSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeTagForApplication(
        @NotBlank(message = "No application id entered. Unable to remove tag.")
        final String id,
        @NotBlank(message = "No tag entered. Unable to remove.")
        final String tag
    ) throws GenieException {
        final ApplicationEntity app = this.findApplication(id);
        final Set<String> tags = app.getApplicationTags();
        tags.remove(tag);
        app.setApplicationTags(tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<Command> getCommandsForApplication(
        @NotBlank(message = "No application id entered. Unable to get commands.")
        final String id,
        final Set<CommandStatus> statuses
    ) throws GenieException {
        if (!this.applicationRepo.exists(id)) {
            throw new GenieNotFoundException("No application with id " + id + " exists.");
        }
        @SuppressWarnings("unchecked")
        final List<CommandEntity> commandEntities = this.commandRepo.findAll(
            JpaCommandSpecs.findCommandsForApplication(id, statuses)
        );
        return commandEntities
            .stream()
            .map(CommandEntity::getDTO)
            .collect(Collectors.toSet());
    }

    /**
     * Helper to find an application entity based on ID.
     *
     * @param id The id of the application to find
     * @return The application entity if one is found
     * @throws GenieNotFoundException If no application is found
     */
    private ApplicationEntity findApplication(final String id)
        throws GenieNotFoundException {
        final ApplicationEntity applicationEntity = this.applicationRepo.findOne(id);
        if (applicationEntity != null) {
            return applicationEntity;
        } else {
            throw new GenieNotFoundException("No application with id " + id);
        }
    }
}
