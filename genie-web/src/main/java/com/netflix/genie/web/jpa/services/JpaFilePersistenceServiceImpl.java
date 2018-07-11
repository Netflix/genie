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

import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
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
 * JPA based implementation of the FilePersistenceService interface.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Transactional
@Slf4j
public class JpaFilePersistenceServiceImpl implements JpaFilePersistenceService {

    private final JpaFileRepository fileRepository;

    /**
     * Constructor.
     *
     * @param fileRepository The repository to use to perform CRUD operations on files
     */
    public JpaFilePersistenceServiceImpl(final JpaFileRepository fileRepository) {
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
                .findUnusedFiles(createdThreshold)
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
    public Optional<FileEntity> getFile(@NotBlank(message = "File path cannot be blank") final String file) {
        return this.fileRepository.findByFile(file);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<FileEntity> getFiles(@NotNull final Set<String> files) {
        return this.fileRepository.findByFileIn(files);
    }
}
