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

import com.netflix.genie.core.jpa.entities.FileEntity;
import com.netflix.genie.core.jpa.repositories.JpaFileRepository;
import com.netflix.genie.core.services.FileService;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Date;
import java.util.stream.Collectors;

/**
 * JPA based implementation of the FileService interface.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Slf4j
@Transactional
public class JpaFileServiceImpl implements FileService {

    private final JpaFileRepository fileRepository;

    /**
     * Constructor.
     *
     * @param fileRepository The repository to use to perform CRUD operations on files
     */
    public JpaFileServiceImpl(final JpaFileRepository fileRepository) {
        this.fileRepository = fileRepository;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createFileIfNotExists(@NotBlank(message = "File path cannot be blank") final String file) {
        if (this.fileRepository.existsByFile(file)) {
            return;
        }

        // Try to create the file
        final FileEntity fileEntity = new FileEntity(file);

        try {
            this.fileRepository.saveAndFlush(fileEntity);
        } catch (final DataIntegrityViolationException e) {
            // Must've been created during the time between exists query and now
            log.error("File expected not to be there but seems to be {}", e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long deleteUnusedFiles(@NotNull final Instant createdThreshold) {
        return this.fileRepository.deleteByIdIn(
            this.fileRepository
                .findUnusedFiles(Date.from(createdThreshold))
                .stream()
                .map(Number::longValue)
                .collect(Collectors.toSet())
        );
    }
}
