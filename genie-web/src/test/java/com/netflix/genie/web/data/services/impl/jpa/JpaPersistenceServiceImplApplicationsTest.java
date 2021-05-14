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
package com.netflix.genie.web.data.services.impl.jpa;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.ApplicationMetadata;
import com.netflix.genie.common.external.dtos.v4.ApplicationRequest;
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.web.data.services.impl.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaRepositories;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.exceptions.checked.PreconditionFailedException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;

import javax.persistence.EntityManager;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Tests for the applications persistence APIs of {@link JpaPersistenceServiceImpl}.
 *
 * @author tgianos
 */
class JpaPersistenceServiceImplApplicationsTest {

    private static final String APP_1_ID = "app1";
    private static final String APP_1_NAME = "tez";
    private static final String APP_1_USER = "tgianos";
    private static final String APP_1_VERSION = "1.2.3";

    private JpaApplicationRepository jpaApplicationRepository;
    private JpaPersistenceServiceImpl persistenceService;

    @BeforeEach
    void setup() {
        this.jpaApplicationRepository = Mockito.mock(JpaApplicationRepository.class);
        final JpaRepositories jpaRepositories = Mockito.mock(JpaRepositories.class);
        Mockito.when(jpaRepositories.getApplicationRepository()).thenReturn(this.jpaApplicationRepository);
        this.persistenceService = new JpaPersistenceServiceImpl(
            Mockito.mock(EntityManager.class),
            jpaRepositories,
            Mockito.mock(BraveTracingComponents.class)
        );
    }

    @Test
    void testGetApplicationNotExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.getApplicationDto(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getApplication(id));
    }

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
            .assertThatExceptionOfType(IdAlreadyExistsException.class)
            .isThrownBy(() -> this.persistenceService.saveApplication(request));
    }

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
        Mockito.when(this.jpaApplicationRepository.getApplicationDto(APP_1_ID)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.updateApplication(APP_1_ID, app));
    }

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
            .assertThatExceptionOfType(PreconditionFailedException.class)
            .isThrownBy(() -> this.persistenceService.updateApplication(id, app));
    }

    @Test
    void testDeleteAllBlocked() {
        final ApplicationEntity applicationEntity = Mockito.mock(ApplicationEntity.class);
        final CommandEntity commandEntity = Mockito.mock(CommandEntity.class);
        Mockito.when(this.jpaApplicationRepository.findAll()).thenReturn(Lists.newArrayList(applicationEntity));
        Mockito.when(applicationEntity.getCommands()).thenReturn(Sets.newHashSet(commandEntity));
        Assertions
            .assertThatExceptionOfType(PreconditionFailedException.class)
            .isThrownBy(() -> this.persistenceService.deleteAllApplications());
    }

    @Test
    void testAddConfigsToApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.addConfigsToResource(id, Sets.newHashSet(), Application.class));
    }

    @Test
    void testUpdateConfigsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.updateConfigsForResource(id, Sets.newHashSet(), Application.class)
            );
    }

    @Test
    void testGetConfigsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getConfigsForResource(id, Application.class));
    }

    @Test
    void testRemoveAllConfigsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.removeAllConfigsForResource(id, Application.class));
    }

    @Test
    void testRemoveConfigForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.removeConfigForResource(id, "something", Application.class));
    }

    @Test
    void testAddDependenciesForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.addDependenciesToResource(id, Sets.newHashSet(), Application.class)
            );
    }

    @Test
    void testUpdateDependenciesForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.updateDependenciesForResource(id, Sets.newHashSet(), Application.class)
            );
    }

    @Test
    void testGetDependenciesForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getDependenciesForResource(id, Application.class));
    }

    @Test
    void testRemoveAllDependenciesForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.removeAllDependenciesForResource(id, Application.class));
    }

    @Test
    void testRemoveDependencyForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.removeDependencyForResource(id, "something", Application.class));
    }

    @Test
    void testAddTagsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.addTagsToResource(id, Sets.newHashSet(), Application.class));
    }

    @Test
    void testUpdateTagsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.updateTagsForResource(id, Sets.newHashSet(), Application.class));
    }

    @Test
    void testGetTagsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getTagsForResource(id, Application.class));
    }

    @Test
    void testRemoveAllTagsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.removeAllTagsForResource(id, Application.class));
    }

    @Test
    void testRemoveTagForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.removeTagForResource(id, "something", Application.class));
    }

    @Test
    void testGetCommandsForApplicationNoApp() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getCommandsForApplication(id, null));
    }
}
