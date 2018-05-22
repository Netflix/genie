/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.jpa.services;

import com.google.common.collect.Sets;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.ClusterEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.entities.UniqueIdEntity;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Base service for other services to extend for common functionality.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Slf4j
@Getter(AccessLevel.PACKAGE)
class JpaBaseService {

    private final JpaTagPersistenceService tagPersistenceService;
    private final JpaFilePersistenceService filePersistenceService;
    private final JpaApplicationRepository applicationRepository;
    private final JpaClusterRepository clusterRepository;
    private final JpaCommandRepository commandRepository;

    /**
     * Constructor.
     *
     * @param tagPersistenceService  The tag persistence service to use
     * @param filePersistenceService The file persistence service to use
     * @param applicationRepository  Application repository to use
     * @param clusterRepository      Cluster repository to use
     * @param commandRepository      Command repository to use
     */
    JpaBaseService(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        this.tagPersistenceService = tagPersistenceService;
        this.filePersistenceService = filePersistenceService;
        this.applicationRepository = applicationRepository;
        this.clusterRepository = clusterRepository;
        this.commandRepository = commandRepository;
    }

    /**
     * Create a file reference in the database and return the attached entity.
     *
     * @param file The file to create
     * @return The file entity that has been persisted in the database
     * @throws GenieRuntimeException If we can't find the entity after creation.
     */
    FileEntity createAndGetFileEntity(@NotBlank(message = "File path cannot be blank") final String file) {
        this.filePersistenceService.createFileIfNotExists(file);
        return this.filePersistenceService.getFile(file).orElseThrow(
            // This shouldn't ever happen as the contract of previous API call states it will exists hence
            // throw a Runtime exception as there is no real recovery
            () -> new GenieRuntimeException("Couldn't find file entity for file " + file)
        );
    }

    /**
     * Create a set of file references in the database and return the set of attached entities.
     *
     * @param files The files to create
     * @return The set of attached entities
     * @throws GenieRuntimeException on error
     */
    Set<FileEntity> createAndGetFileEntities(final Set<String> files) {
        final Set<FileEntity> fileEntities = Sets.newHashSet();
        for (final String file : files) {
            fileEntities.add(this.createAndGetFileEntity(file));
        }
        return fileEntities;
    }

    /**
     * Create a tag reference in the database and return the attached entity.
     *
     * @param tag The file to create
     * @return The tag entity that has been persisted in the database
     * @throws GenieRuntimeException on error
     */
    TagEntity createAndGetTagEntity(@NotBlank(message = "Tag cannot be blank") final String tag) {
        this.tagPersistenceService.createTagIfNotExists(tag);
        return this.tagPersistenceService.getTag(tag).orElseThrow(
            // This shouldn't ever happen as the contract of previous API call states it will exists hence
            // throw a Runtime exception as there is no real recovery
            () -> new GenieRuntimeException("Couldn't find tag entity for tag " + tag)
        );
    }

    /**
     * Create a set of tag references in the database and return the set of attached entities.
     *
     * @param tags The tags to create
     * @return The set of attached entities
     * @throws GenieRuntimeException on error
     */
    Set<TagEntity> createAndGetTagEntities(final Set<String> tags) {
        final Set<TagEntity> tagEntities = Sets.newHashSet();
        for (final String tag : tags) {
            tagEntities.add(this.createAndGetTagEntity(tag));
        }
        return tagEntities;
    }

    /**
     * Set the unique id and other related fields for an entity.
     *
     * @param entity      The entity to set the unique id for
     * @param requestedId The id requested if there was one. Null if not.
     * @param <E>         The entity type which must extend {@link UniqueIdEntity}
     */
    <E extends UniqueIdEntity> void setUniqueId(final E entity, @Nullable final String requestedId) {
        if (requestedId != null) {
            entity.setUniqueId(requestedId);
            entity.setRequestedId(true);
        } else {
            entity.setUniqueId(UUID.randomUUID().toString());
            entity.setRequestedId(false);
        }
    }

    /**
     * Set the resources (configs, dependencies, setup file) for an Entity.
     *
     * @param resources            The resources to set
     * @param configsConsumer      The consumer method for the created config file entities
     * @param dependenciesConsumer The consumer method for the created dependency file entities
     * @param setupFileConsumer    The consumer method for the created setup file reference
     */
    void setEntityResources(
        final ExecutionEnvironment resources,
        final Consumer<Set<FileEntity>> configsConsumer,
        final Consumer<Set<FileEntity>> dependenciesConsumer,
        final Consumer<FileEntity> setupFileConsumer
    ) {
        // Save all the unowned entities first to avoid unintended flushes
        configsConsumer.accept(this.createAndGetFileEntities(resources.getConfigs()));
        dependenciesConsumer.accept(this.createAndGetFileEntities(resources.getDependencies()));
        setupFileConsumer.accept(
            resources.getSetupFile().isPresent()
                ? this.createAndGetFileEntity(resources.getSetupFile().get())
                : null
        );
    }

    /**
     * Set the tag entities created into the consumer.
     *
     * @param tags         The tags to create and set
     * @param tagsConsumer Where to store the created tags
     */
    void setEntityTags(final Set<String> tags, final Consumer<Set<TagEntity>> tagsConsumer) {
        tagsConsumer.accept(this.createAndGetTagEntities(tags));
    }

    /**
     * Retrieve an application entity. If this API is called within an active transaction the returned entity will
     * remain attached to the persistence context, otherwise it will be detached and any modifications will be lost.
     *
     * @param id The id of the application to retrieve from the database
     * @return The {@link ApplicationEntity} if a match for the {@code id} is found or {@link Optional#empty()}
     */
    Optional<ApplicationEntity> getApplicationEntity(@NotBlank(message = "No application id entered") final String id) {
        return this.applicationRepository.findByUniqueId(id);
    }

    /**
     * Retrieve a cluster entity. If this API is called within an active transaction the returned entity will
     * remain attached to the persistence context, otherwise it will be detached and any modifications will be lost.
     *
     * @param id The id of the cluster to retrieve from the database
     * @return The {@link ClusterEntity} if a match for the {@code id} is found or {@link Optional#empty()}
     */
    Optional<ClusterEntity> getClusterEntity(@NotBlank(message = "No cluster id entered") final String id) {
        return this.clusterRepository.findByUniqueId(id);
    }

    /**
     * Retrieve a command entity. If this API is called within an active transaction the returned entity will
     * remain attached to the persistence context, otherwise it will be detached and any modifications will be lost.
     *
     * @param id The id of the command to retrieve from the database
     * @return The {@link CommandEntity} if a match for the {@code id} is found or {@link Optional#empty()}
     */
    Optional<CommandEntity> getCommandEntity(@NotBlank(message = "No command id entered") final String id) {
        return this.commandRepository.findByUniqueId(id);
    }
}
