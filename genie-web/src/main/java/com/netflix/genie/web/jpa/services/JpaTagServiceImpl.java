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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.services.TagService;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Transactional;

/**
 * JPA based implementation of the TagService interface.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Slf4j
@Transactional
public class JpaTagServiceImpl implements TagService {

    private final JpaTagRepository tagRepository;

    /**
     * Constructor.
     *
     * @param tagRepository The repository to use to perform CRUD operations on tags
     */
    public JpaTagServiceImpl(final JpaTagRepository tagRepository) {
        this.tagRepository = tagRepository;
    }

    /**
     * {@inheritDoc}
     */
    @Override
//    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void createTagIfNotExists(@NotBlank final String tag) throws GenieException {
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
}
