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
package com.netflix.genie.web.data.services.jpa;

import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.external.dtos.v4.ApplicationMetadata;
import com.netflix.genie.common.external.dtos.v4.ApplicationRequest;
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.web.data.entities.FileEntity;
import com.netflix.genie.web.data.services.ApplicationPersistenceService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the {@link JpaFilePersistenceServiceImpl} class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@DatabaseTearDown("cleanup.xml")
class JpaFilePersistenceServiceImplIntegrationTest extends DBIntegrationTestBase {

    // This needs to be injected as a Spring Bean otherwise transactions don't work as there is no proxy
    @Autowired
    private JpaFilePersistenceServiceImpl filePersistenceService;

    @Autowired
    private ApplicationPersistenceService applicationPersistenceService;

    /**
     * Make sure that no matter how many times we try to create a file it doesn't throw an error on duplicate key it
     * just does nothing.
     */
    @Test
    void canCreateFileIfNotExists() {
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(0L);
        final String file = UUID.randomUUID().toString();
        Assertions.assertThat(this.filePersistenceService.getFile(file)).isNotPresent();
        this.filePersistenceService.createFileIfNotExists(file);
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(1L);
        Assertions.assertThat(this.fileRepository.existsByFile(file)).isTrue();
        Assertions
            .assertThat(this.filePersistenceService.getFile(file))
            .isPresent()
            .get()
            .extracting(FileEntity::getFile)
            .isEqualTo(file);

        // Try again with the same file
        this.filePersistenceService.createFileIfNotExists(file);
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(1L);
        Assertions.assertThat(this.fileRepository.existsByFile(file)).isTrue();
        Assertions
            .assertThat(this.filePersistenceService.getFile(file))
            .isPresent()
            .get()
            .extracting(FileEntity::getFile)
            .isEqualTo(file);
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(1L);
    }

    @Test
    void canDeleteUnusedFiles() throws GenieException {
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(0L);
        final String file1 = UUID.randomUUID().toString();
        final String file2 = UUID.randomUUID().toString();
        final String file3 = UUID.randomUUID().toString();
        final String file4 = UUID.randomUUID().toString();
        final String file5 = UUID.randomUUID().toString();

        this.filePersistenceService.createFileIfNotExists(file1);
        this.filePersistenceService.createFileIfNotExists(file4);

        final ApplicationRequest app = new ApplicationRequest.Builder(
            new ApplicationMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ApplicationStatus.ACTIVE
            ).build())
            .withResources(
                new ExecutionEnvironment(
                    Sets.newHashSet(file3),
                    Sets.newHashSet(file2),
                    file5
                )
            )
            .build();

        final String appId = this.applicationPersistenceService.createApplication(app);

        Assertions.assertThat(this.fileRepository.existsByFile(file1)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file2)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file3)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file4)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file5)).isTrue();

        Assertions.assertThat(this.filePersistenceService.deleteUnusedFiles(Instant.now())).isEqualTo(2L);

        Assertions.assertThat(this.fileRepository.existsByFile(file1)).isFalse();
        Assertions.assertThat(this.fileRepository.existsByFile(file2)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file3)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file4)).isFalse();
        Assertions.assertThat(this.fileRepository.existsByFile(file5)).isTrue();

        this.applicationPersistenceService.deleteApplication(appId);
    }

    @Test
    void canFindFiles() {
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(0L);
        final String file1 = UUID.randomUUID().toString();
        final String file2 = UUID.randomUUID().toString();
        this.filePersistenceService.createFileIfNotExists(file1);
        final FileEntity fileEntity1
            = this.filePersistenceService.getFile(file1).orElseThrow(IllegalArgumentException::new);
        this.filePersistenceService.createFileIfNotExists(file2);
        final FileEntity fileEntity2
            = this.filePersistenceService.getFile(file2).orElseThrow(IllegalArgumentException::new);

        Set<FileEntity> files = this.filePersistenceService.getFiles(Sets.newHashSet(file1, file2));
        Assertions.assertThat(files).hasSize(2).contains(fileEntity1, fileEntity2);

        files = this.filePersistenceService.getFiles(Sets.newHashSet(file1, file2, UUID.randomUUID().toString()));
        Assertions.assertThat(files).hasSize(2).contains(fileEntity1, fileEntity2);

        files = this.filePersistenceService.getFiles(Sets.newHashSet(file1, UUID.randomUUID().toString()));
        Assertions.assertThat(files).hasSize(1).contains(fileEntity1);
    }
}
