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

import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JPA based implementation of the TagPersistenceService interface.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Transactional
@Slf4j
public class JpaTagPersistenceServiceImpl implements JpaTagPersistenceService {

    private final JpaTagRepository tagRepository;

    /**
     * Constructor.
     *
     * @param tagRepository The repository to use to perform CRUD operations on tags
     */
    public JpaTagPersistenceServiceImpl(final JpaTagRepository tagRepository) {
        this.tagRepository = tagRepository;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTagIfNotExists(@NotBlank(message = "Tag cannot be blank") final String tag) {
        if (this.tagRepository.existsByTag(tag)) {
            return;
        }

        try {
            this.tagRepository.saveAndFlush(new TagEntity(tag));
        } catch (final DataIntegrityViolationException e) {
            // Must've been created during the time between exists query and now
            log.error("Tag expected not to be there but seems to be {}", e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long deleteUnusedTags(@NotNull final Instant createdThreshold) {
        return this.tagRepository.deleteByIdIn(
            this.tagRepository
                .findUnusedTags(createdThreshold)
                .stream()
                .map(Number::longValue)
                .collect(Collectors.toSet())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Optional<TagEntity> getTag(@NotBlank(message = "Tag string to find can't be blank") final String tag) {
        return this.tagRepository.findByTag(tag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<TagEntity> getTags(@NotNull final Set<String> tags) {
        return this.tagRepository.findByTagIn(tags);
    }
}
