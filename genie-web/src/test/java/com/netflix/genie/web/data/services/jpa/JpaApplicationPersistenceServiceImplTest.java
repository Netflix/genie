/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.ApplicationMetadata;
import com.netflix.genie.common.external.dtos.v4.ApplicationRequest;
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.web.data.entities.ApplicationEntity;
import com.netflix.genie.web.data.entities.CommandEntity;
import com.netflix.genie.web.data.repositories.jpa.JpaApplicationRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaClusterRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCommandRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Tests for {@link JpaApplicationPersistenceServiceImpl}.
 *
 * @author tgianos
 */
class JpaApplicationPersistenceServiceImplTest {

    private static final String APP_1_ID = "app1";
    private static final String APP_1_NAME = "tez";
    private static final String APP_1_USER = "tgianos";
    private static final String APP_1_VERSION = "1.2.3";

    private JpaApplicationRepository jpaApplicationRepository;
    private JpaApplicationPersistenceServiceImpl appService;

    /**
     * Setup the tests.
     */
    @BeforeEach
    void setup() {
        this.jpaApplicationRepository = Mockito.mock(JpaApplicationRepository.class);
        this.appService = new JpaApplicationPersistenceServiceImpl(
            Mockito.mock(JpaTagPersistenceService.class),
            Mockito.mock(JpaFilePersistenceService.class),
            this.jpaApplicationRepository,
            Mockito.mock(JpaClusterRepository.class),
            Mockito.mock(JpaCommandRepository.class)
        );
    }

    /**
     * Test the get application method.
     */
    @Test
    void testGetApplicationNotExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.getApplication(id));
    }

    /**
     * Test to make sure an exception is thrown when application already exists.
     */
    @Test
    void testCreateApplicationAlreadyExists() {
        final ApplicationRequest request = new ApplicationRequest.Builder(
            new ApplicationMetadata.Builder(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
            )
                .build()
        )
            .withRequestedId(APP_1_ID)
            .build();

        Mockito
            .when(this.jpaApplicationRepository.save(Mockito.any(ApplicationEntity.class)))
            .thenThrow(new DuplicateKeyException("Duplicate Key"));
        Assertions
            .assertThatExceptionOfType(GenieConflictException.class)
            .isThrownBy(() -> this.appService.createApplication(request));
    }

    /**
     * Test to update an application.
     */
    @Test
    void testUpdateApplicationNoAppExists() {
        final Application app = new Application(
            APP_1_ID,
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ApplicationMetadata.Builder(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
            )
                .build()
        );
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.updateApplication(id, app));
    }

    /**
     * Test to update an application.
     */
    @Test
    void testUpdateApplicationIdsDontMatch() {
        final String id = UUID.randomUUID().toString();
        final Application app = new Application(
            UUID.randomUUID().toString(),
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ApplicationMetadata.Builder(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
            )
                .build()
        );
        Mockito.when(this.jpaApplicationRepository.existsByUniqueId(id)).thenReturn(true);
        Assertions
            .assertThatExceptionOfType(GenieBadRequestException.class)
            .isThrownBy(() -> this.appService.updateApplication(id, app));
    }

    /**
     * Test delete all when still in a relationship with a command.
     */
    @Test
    void testDeleteAllBlocked() {
        final ApplicationEntity applicationEntity
            = Mockito.mock(ApplicationEntity.class);
        final CommandEntity commandEntity = Mockito.mock(CommandEntity.class);
        Mockito.when(this.jpaApplicationRepository.findAll()).thenReturn(Lists.newArrayList(applicationEntity));
        Mockito.when(applicationEntity.getCommands()).thenReturn(Sets.newHashSet(commandEntity));
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.appService.deleteAllApplications());
    }

    /**
     * Test delete all still runs if commands is null.
     *
     * @throws GenieException For any problem
     */
    @Test
    void testDeleteAllNullCommands() throws GenieException {
        final ApplicationEntity applicationEntity = Mockito.mock(ApplicationEntity.class);
        Mockito.when(this.jpaApplicationRepository.findAll()).thenReturn(Lists.newArrayList(applicationEntity));
        Mockito.when(applicationEntity.getCommands()).thenReturn(null);
        this.appService.deleteAllApplications();
        Mockito.verify(this.jpaApplicationRepository, Mockito.times(1)).deleteAll();
    }

    /**
     * Test delete.
     */
    @Test
    void testDeleteNoAppToDelete() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.deleteApplication(id));
    }

    /**
     * Test add configurations to application.
     */
    @Test
    void testAddConfigsToApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.addConfigsToApplication(id, Sets.newHashSet()));
    }

    /**
     * Test update configurations for application.
     */
    @Test
    void testUpdateConfigsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.updateConfigsForApplication(id, Sets.newHashSet()));
    }

    /**
     * Test get configurations to application.
     */
    @Test
    void testGetConfigsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.getConfigsForApplication(id));
    }

    /**
     * Test remove all configurations for application.
     */
    @Test
    void testRemoveAllConfigsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.removeAllConfigsForApplication(id));
    }

    /**
     * Test remove configuration for application.
     */
    @Test
    void testRemoveConfigForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.removeConfigForApplication(id, "something"));
    }

    /**
     * Test add jars to application.
     */
    @Test
    void testAddJarsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.addDependenciesForApplication(id, Sets.newHashSet()));
    }

    /**
     * Test update jars for application.
     */
    @Test
    void testUpdateDependenciesForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.updateDependenciesForApplication(id, Sets.newHashSet()));
    }

    /**
     * Test get dependencies to application.
     */
    @Test
    void testGetDependenciesForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.getDependenciesForApplication(id));
    }

    /**
     * Test remove all jars for application.
     */
    @Test
    void testRemoveAllDependenciesForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.removeAllDependenciesForApplication(id));
    }

    /**
     * Test remove configuration for application.
     */
    @Test
    void testRemoveDependenciesForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.removeDependencyForApplication(id, "something"));
    }

    /**
     * Test add tags to application.
     */
    @Test
    void testAddTagsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.addTagsForApplication(id, Sets.newHashSet()));
    }

    /**
     * Test update tags for application.
     */
    @Test
    void testUpdateTagsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.updateTagsForApplication(id, Sets.newHashSet()));
    }

    /**
     * Test get tags to application.
     */
    @Test
    void testGetTagsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.getTagsForApplication(id));
    }

    /**
     * Test remove all tags for application.
     */
    @Test
    void testRemoveAllTagsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.removeAllTagsForApplication(id));
    }

    /**
     * Test remove configuration for application.
     */
    @Test
    void testRemoveTagForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.removeTagForApplication(id, "something"));
    }

    /**
     * Test the Get commands for application method.
     */
    @Test
    void testGetCommandsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.getCommandsForApplication(id, null));
    }
}
