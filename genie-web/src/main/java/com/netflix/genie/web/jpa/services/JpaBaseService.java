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
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.entities.UniqueIdEntity;
import com.netflix.genie.web.services.FilePersistenceService;
import com.netflix.genie.web.services.TagPersistenceService;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import java.util.Set;
import java.util.UUID;

/**
 * Base service for other services to extend for common functionality.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Slf4j
@Getter(AccessLevel.PACKAGE)
class JpaBaseService {

    private static final char COMMA = ',';
    private static final String EMPTY_STRING = "";

    private final TagPersistenceService tagPersistenceService;
    private final FilePersistenceService filePersistenceService;

    /**
     * Constructor.
     *
     * @param tagPersistenceService  The tag service to use
     * @param filePersistenceService The file service to use
     */
    JpaBaseService(
        final TagPersistenceService tagPersistenceService,
        final FilePersistenceService filePersistenceService
    ) {
        this.tagPersistenceService = tagPersistenceService;
        this.filePersistenceService = filePersistenceService;
    }

    /**
     * Create a file reference in the database and return the attached entity.
     *
     * @param file The file to create
     * @return The file entity that has been persisted in the database
     * @throws GenieException on error
     */
    FileEntity createAndGetFileEntity(
        @NotBlank(message = "File path cannot be blank") final String file
    ) throws GenieException {
        this.filePersistenceService.createFileIfNotExists(file);
        return this.filePersistenceService.getFile(file).orElseThrow(
            () -> new GenieNotFoundException("Couldn't find file entity for file " + file)
        );
    }

    /**
     * Create a set of file references in the database and return the set of attached entities.
     *
     * @param files The files to create
     * @return The set of attached entities
     * @throws GenieException on error
     */
    Set<FileEntity> createAndGetFileEntities(final Set<String> files) throws GenieException {
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
     * @throws GenieException on error
     */
    TagEntity createAndGetTagEntity(
        @NotBlank(message = "Tag cannot be blank") final String tag
    ) throws GenieException {
        this.tagPersistenceService.createTagIfNotExists(tag);
        return this.tagPersistenceService.getTag(tag).orElseThrow(
            () -> new GenieNotFoundException("Couldn't find tag entity for tag " + tag)
        );
    }

    /**
     * Create a set of tag references in the database and return the set of attached entities.
     *
     * @param tags The tags to create
     * @return The set of attached entities
     * @throws GenieException on error
     */
    Set<TagEntity> createAndGetTagEntities(final Set<String> tags) throws GenieException {
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
}
