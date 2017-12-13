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
package com.netflix.genie.core.jpa.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.util.GenieDateFormat;
import com.netflix.genie.core.jpa.entities.FileEntity;
import com.netflix.genie.core.jpa.entities.TagEntity;
import com.netflix.genie.core.jpa.repositories.JpaFileRepository;
import com.netflix.genie.core.jpa.repositories.JpaTagRepository;
import com.netflix.genie.core.services.FileService;
import com.netflix.genie.core.services.TagService;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.text.DateFormat;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * Base service for other services to extend for common functionality.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Slf4j
@Getter(AccessLevel.PACKAGE)
class JpaBaseService {

    static final ObjectMapper MAPPER;
    static final String GENIE_TAG_NAMESPACE = "genie.";
    static final String GENIE_ID_TAG_NAMESPACE = GENIE_TAG_NAMESPACE + "id:";
    static final String GENIE_NAME_TAG_NAMESPACE = GENIE_TAG_NAMESPACE + "name:";
    private static final char COMMA = ',';
    private static final String EMPTY_STRING = "";

    static {
        final DateFormat iso8601 = new GenieDateFormat();
        iso8601.setTimeZone(TimeZone.getTimeZone("UTC"));
        MAPPER = new ObjectMapper().registerModule(new Jdk8Module()).setDateFormat(iso8601);
    }

    private final TagService tagService;
    private final JpaTagRepository tagRepository;
    private final FileService fileService;
    private final JpaFileRepository fileRepository;

    /**
     * Constructor.
     *
     * @param tagService     The tag service to use
     * @param tagRepository  The tag repository to use
     * @param fileService    The file service to use
     * @param fileRepository The file repository to use
     */
    JpaBaseService(
        final TagService tagService,
        final JpaTagRepository tagRepository,
        final FileService fileService,
        final JpaFileRepository fileRepository
    ) {
        this.tagService = tagService;
        this.tagRepository = tagRepository;
        this.fileService = fileService;
        this.fileRepository = fileRepository;
    }

    /**
     * Create a file reference in the database and return the attached entity.
     *
     * @param file The file to create
     * @return The file entity that has been persisted in the database
     * @throws GenieException on error
     */
    FileEntity createAndGetFileEntity(final String file) throws GenieException {
        this.fileService.createFileIfNotExists(file);
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
    TagEntity createAndGetTagEntity(final String tag) throws GenieException {
        this.tagService.createTagIfNotExists(tag);
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

    /**
     * Get the tags with the current genie.id and genie.name tags added into the set.
     *
     * @param tags The set of tags to modify
     * @param id   The id of the entity
     * @param name The name of the entity
     * @throws GenieException On any exception
     */
    void setFinalTags(final Set<TagEntity> tags, final String id, final String name) throws GenieException {
        // Make sure the id tag is there. Should never be updated
        final Set<String> idTags = tags
            .stream()
            .filter(tagEntity -> tagEntity.getTag().startsWith(GENIE_ID_TAG_NAMESPACE))
            .map(TagEntity::getTag)
            .collect(Collectors.toSet());
        if (idTags.size() > 1) {
            throw new GenieServerException(
                "Should only have one id tag instead have: " + idTags
                    .stream()
                    .reduce((one, two) -> one + COMMA + two)
                    .orElse(EMPTY_STRING));
        } else if (idTags.isEmpty()) {
            tags.add(this.createAndGetTagEntity(GENIE_ID_TAG_NAMESPACE + id));
        }

        final String nameTag = GENIE_NAME_TAG_NAMESPACE + name;

        // Remove any name tags that aren't the one we want
        tags.removeAll(
            tags
                .stream()
                .filter(
                    tagEntity -> {
                        final String tag = tagEntity.getTag();
                        return tag.startsWith(GENIE_NAME_TAG_NAMESPACE) && !tag.equals(nameTag);
                    }
                )
                .collect(Collectors.toSet())
        );

        // Check if the tags contains the name tag now
        if (tags.stream().filter(tagEntity -> tagEntity.getTag().equals(nameTag)).count() < 1) {
            tags.add(this.createAndGetTagEntity(nameTag));
        }
    }
}
