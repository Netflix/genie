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
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.services.FilePersistenceService;
import com.netflix.genie.web.services.TagPersistenceService;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import java.util.Set;

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
    private final JpaTagRepository tagRepository;
    private final FilePersistenceService filePersistenceService;
    private final JpaFileRepository fileRepository;

    /**
     * Constructor.
     *
     * @param tagPersistenceService  The tag service to use
     * @param tagRepository          The tag repository to use
     * @param filePersistenceService The file service to use
     * @param fileRepository         The file repository to use
     */
    JpaBaseService(
        final TagPersistenceService tagPersistenceService,
        final JpaTagRepository tagRepository,
        final FilePersistenceService filePersistenceService,
        final JpaFileRepository fileRepository
    ) {
        this.tagPersistenceService = tagPersistenceService;
        this.tagRepository = tagRepository;
        this.filePersistenceService = filePersistenceService;
        this.fileRepository = fileRepository;
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
        return this.fileRepository.findByFile(file).orElseThrow(
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
        return this.tagRepository.findByTag(tag).orElseThrow(
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
}
