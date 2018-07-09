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

import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dto.v4.ApplicationMetadata;
import com.netflix.genie.common.internal.dto.v4.ApplicationRequest;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the JpaFilePersistenceServiceImpl class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(IntegrationTest.class)
@DatabaseTearDown("cleanup.xml")
public class JpaFilePersistenceServiceImplIntegrationTest extends DBIntegrationTestBase {

    // This needs to be injected as a Spring Bean otherwise transactions don't work as there is no proxy
    @Autowired
    private JpaFilePersistenceService filePersistenceService;

    @Autowired
    private ApplicationPersistenceService applicationPersistenceService;

    /**
     * Make sure that no matter how many times we try to create a file it doesn't throw an error on duplicate key it
     * just does nothing.
     */
    @Test
    public void canCreateFileIfNotExists() {
        Assert.assertThat(this.fileRepository.count(), Matchers.is(0L));
        final String file = UUID.randomUUID().toString();
        Assert.assertFalse(this.filePersistenceService.getFile(file).isPresent());
        this.filePersistenceService.createFileIfNotExists(file);
        Assert.assertThat(this.fileRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.fileRepository.existsByFile(file));
        final Optional<FileEntity> fileEntityOptional = this.filePersistenceService.getFile(file);
        Assert.assertTrue(fileEntityOptional.isPresent());
        final FileEntity fileEntity = fileEntityOptional.get();
        Assert.assertThat(fileEntity.getFile(), Matchers.is(file));

        // Try again with the same file
        this.filePersistenceService.createFileIfNotExists(file);
        Assert.assertThat(this.fileRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.fileRepository.existsByFile(file));
        final Optional<FileEntity> fileEntityOptional2 = this.filePersistenceService.getFile(file);
        Assert.assertTrue(fileEntityOptional2.isPresent());
        final FileEntity fileEntity2 = fileEntityOptional2.get();
        Assert.assertThat(fileEntity2.getFile(), Matchers.is(file));

        // Make sure the ids are still equal
        Assert.assertThat(fileEntity2.getId(), Matchers.is(fileEntity.getId()));
    }

    /**
     * Make sure we can delete files that aren't attached to other resources.
     *
     * @throws GenieException on error
     */
    @Test
    public void canDeleteUnusedFiles() throws GenieException {
        Assert.assertThat(this.fileRepository.count(), Matchers.is(0L));
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

        Assert.assertTrue(this.fileRepository.existsByFile(file1));
        Assert.assertTrue(this.fileRepository.existsByFile(file2));
        Assert.assertTrue(this.fileRepository.existsByFile(file3));
        Assert.assertTrue(this.fileRepository.existsByFile(file4));
        Assert.assertTrue(this.fileRepository.existsByFile(file5));

        Assert.assertThat(this.filePersistenceService.deleteUnusedFiles(Instant.now()), Matchers.is(2L));

        Assert.assertFalse(this.fileRepository.existsByFile(file1));
        Assert.assertTrue(this.fileRepository.existsByFile(file2));
        Assert.assertTrue(this.fileRepository.existsByFile(file3));
        Assert.assertFalse(this.fileRepository.existsByFile(file4));
        Assert.assertTrue(this.fileRepository.existsByFile(file5));

        this.applicationPersistenceService.deleteApplication(appId);
    }

    /**
     * Make sure we can find files.
     */
    @Test
    public void canFindFiles() {
        Assert.assertThat(this.fileRepository.count(), Matchers.is(0L));
        final String file1 = UUID.randomUUID().toString();
        final String file2 = UUID.randomUUID().toString();
        this.filePersistenceService.createFileIfNotExists(file1);
        final FileEntity fileEntity1
            = this.filePersistenceService.getFile(file1).orElseThrow(IllegalArgumentException::new);
        this.filePersistenceService.createFileIfNotExists(file2);
        final FileEntity fileEntity2
            = this.filePersistenceService.getFile(file2).orElseThrow(IllegalArgumentException::new);

        Set<FileEntity> files = this.filePersistenceService.getFiles(Sets.newHashSet(file1, file2));
        Assert.assertThat(files.size(), Matchers.is(2));
        Assert.assertThat(files, Matchers.hasItems(fileEntity1, fileEntity2));

        files = this.filePersistenceService.getFiles(Sets.newHashSet(file1, file2, UUID.randomUUID().toString()));
        Assert.assertThat(files.size(), Matchers.is(2));
        Assert.assertThat(files, Matchers.hasItems(fileEntity1, fileEntity2));

        files = this.filePersistenceService.getFiles(Sets.newHashSet(file1, UUID.randomUUID().toString()));
        Assert.assertThat(files.size(), Matchers.is(1));
        Assert.assertThat(files, Matchers.hasItem(fileEntity1));
    }
}
