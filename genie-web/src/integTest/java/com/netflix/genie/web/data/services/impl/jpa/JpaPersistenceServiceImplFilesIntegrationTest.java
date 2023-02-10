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
package com.netflix.genie.web.data.services.impl.jpa;

import com.google.common.collect.Sets;
import com.netflix.genie.common.internal.dtos.ApplicationMetadata;
import com.netflix.genie.common.internal.dtos.ApplicationRequest;
import com.netflix.genie.common.internal.dtos.ApplicationStatus;
import com.netflix.genie.common.internal.dtos.ExecutionEnvironment;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.web.data.services.impl.jpa.entities.FileEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

/**
 * Integration tests for the {@link JpaPersistenceServiceImpl} focusing on file APIs.
 *
 * @author tgianos
 * @since 3.3.0
 */
class JpaPersistenceServiceImplFilesIntegrationTest extends JpaPersistenceServiceIntegrationTestBase {

    @Test
    void canDeleteUnusedFiles() throws GenieCheckedException {
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(0L);
        final String file1 = UUID.randomUUID().toString();
        final String file2 = UUID.randomUUID().toString();
        final String file3 = UUID.randomUUID().toString();
        final String file4 = UUID.randomUUID().toString();
        final String file5 = UUID.randomUUID().toString();

        this.fileRepository.saveAndFlush(new FileEntity(file1));
        this.fileRepository.saveAndFlush(new FileEntity(file4));

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

        // Create a relationship between files and some resource in the system that should block them from being deleted
        this.service.saveApplication(app);

        Assertions.assertThat(this.fileRepository.existsByFile(file1)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file2)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file3)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file4)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file5)).isTrue();

        Assertions.assertThat(this.service.deleteUnusedFiles(Instant.EPOCH, Instant.now(), 10)).isEqualTo(2L);

        Assertions.assertThat(this.fileRepository.existsByFile(file1)).isFalse();
        Assertions.assertThat(this.fileRepository.existsByFile(file2)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file3)).isTrue();
        Assertions.assertThat(this.fileRepository.existsByFile(file4)).isFalse();
        Assertions.assertThat(this.fileRepository.existsByFile(file5)).isTrue();
    }
}
