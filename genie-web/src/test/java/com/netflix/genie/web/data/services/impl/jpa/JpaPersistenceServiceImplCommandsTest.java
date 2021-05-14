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
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.CommandMetadata;
import com.netflix.genie.common.external.dtos.v4.CommandRequest;
import com.netflix.genie.common.external.dtos.v4.CommandStatus;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaCriterionRepository;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Tests for the {@link JpaPersistenceServiceImpl} command specific functionality.
 *
 * @author tgianos
 * @since 2.0.0
 */
class JpaPersistenceServiceImplCommandsTest {

    private static final String COMMAND_1_ID = "command1";
    private static final String COMMAND_1_NAME = "pig_13_prod";
    private static final String COMMAND_1_USER = "tgianos";
    private static final String COMMAND_1_VERSION = "1.2.3";
    private static final List<String> COMMAND_1_EXECUTABLE = Lists.newArrayList("pig");
    private static final long COMMAND_1_CHECK_DELAY = 18000L;

    private static final String COMMAND_2_ID = "command2";

    private JpaPersistenceServiceImpl service;
    private JpaCommandRepository jpaCommandRepository;
    private JpaApplicationRepository jpaApplicationRepository;

    @BeforeEach
    void setup() {
        this.jpaCommandRepository = Mockito.mock(JpaCommandRepository.class);
        this.jpaApplicationRepository = Mockito.mock(JpaApplicationRepository.class);
        final JpaRepositories jpaRepositories = Mockito.mock(JpaRepositories.class);
        Mockito.when(jpaRepositories.getApplicationRepository()).thenReturn(this.jpaApplicationRepository);
        Mockito.when(jpaRepositories.getCommandRepository()).thenReturn(this.jpaCommandRepository);
        Mockito.when(jpaRepositories.getCriterionRepository()).thenReturn(Mockito.mock(JpaCriterionRepository.class));
        this.service = new JpaPersistenceServiceImpl(
            Mockito.mock(EntityManager.class),
            jpaRepositories,
            Mockito.mock(BraveTracingComponents.class)
        );
    }

    @Test
    void testGetCommandNotExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.getCommandDto(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getCommand(id));
    }

    @Test
    void testCreateCommandAlreadyExists() {
        final CommandRequest command = new CommandRequest.Builder(
            new CommandMetadata.Builder(
                COMMAND_1_NAME,
                COMMAND_1_USER,
                COMMAND_1_VERSION,
                CommandStatus.ACTIVE
            )
                .build(),
            COMMAND_1_EXECUTABLE
        )
            .withRequestedId(COMMAND_1_ID)
            .withCheckDelay(COMMAND_1_CHECK_DELAY)
            .build();

        Mockito
            .when(this.jpaCommandRepository.save(Mockito.any(CommandEntity.class)))
            .thenThrow(new DuplicateKeyException("Duplicate Key"));
        Assertions
            .assertThatExceptionOfType(IdAlreadyExistsException.class)
            .isThrownBy(() -> this.service.saveCommand(command));
    }

    @Test
    void testUpdateCommandNoCommandExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.getCommandDto(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(
                () -> this.service.updateCommand(
                    id,
                    new Command(
                        id,
                        Instant.now(),
                        Instant.now(),
                        new ExecutionEnvironment(null, null, null),
                        new CommandMetadata.Builder(
                            " ",
                            " ",
                            " ",
                            CommandStatus.ACTIVE
                        ).build(),
                        Lists.newArrayList(UUID.randomUUID().toString()),
                        null,
                        1803L,
                        null
                    )
                )
            );
    }

    @Test
    void testUpdateCommandIdsDontMatch() {
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(UUID.randomUUID().toString());
        Assertions
            .assertThatExceptionOfType(PreconditionFailedException.class)
            .isThrownBy(() -> this.service.updateCommand(COMMAND_2_ID, command));
    }

    @Test
    void testAddConfigsToCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.addConfigsToResource(id, Sets.newHashSet(), Command.class));
    }

    @Test
    void testUpdateConfigsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.updateConfigsForResource(id, Sets.newHashSet(), Command.class));
    }

    @Test
    void testGetConfigsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getConfigsForResource(id, Command.class));
    }

    @Test
    void testRemoveAllConfigsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeAllConfigsForResource(id, Command.class));
    }

    @Test
    void testRemoveConfigForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeConfigForResource(id, "something", Command.class));
    }

    @Test
    void testAddDependenciesForCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.addDependenciesToResource(id, Sets.newHashSet(), Command.class));
    }

    @Test
    void testUpdateDependenciesForCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.updateDependenciesForResource(id, Sets.newHashSet(), Command.class));
    }

    @Test
    void testGetDependenciesForCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getDependenciesForResource(id, Command.class));
    }

    @Test
    void testRemoveAllDependenciesForCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeAllDependenciesForResource(id, Command.class));
    }

    @Test
    void testRemoveDependencyForCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeDependencyForResource(id, "something", Command.class));
    }

    @Test
    void testSetApplicationsForCommandNoAppId() {
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(
                () -> this.service.setApplicationsForCommand(
                    COMMAND_2_ID,
                    Lists.newArrayList(UUID.randomUUID().toString())
                )
            );
    }

    @Test
    void testSetApplicationsForCommandNoCommandExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Mockito.when(this.jpaApplicationRepository.existsByUniqueId(Mockito.anyString())).thenReturn(true);
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(
                () -> this.service.setApplicationsForCommand(id, Lists.newArrayList(UUID.randomUUID().toString()))
            );
    }

    @Test
    void testSetApplicationsForCommandNoAppExists() {
        final String appId = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.existsByUniqueId(appId)).thenReturn(false);
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.setApplicationsForCommand(COMMAND_2_ID, Lists.newArrayList(appId)));
    }

    @Test
    void cantUpdateApplicationsForCommandWithDuplicates() {
        final String appId1 = UUID.randomUUID().toString();
        final String appId2 = UUID.randomUUID().toString();
        final List<String> appIds = Lists.newArrayList(appId1, appId2, appId1);
        Assertions
            .assertThatExceptionOfType(PreconditionFailedException.class)
            .isThrownBy(() -> this.service.setApplicationsForCommand(COMMAND_1_ID, appIds));
    }

    @Test
    void testGetApplicationsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getApplicationsForCommand(id));
    }

    @Test
    void testRemoveApplicationsForCommandNoCommandExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeApplicationsForCommand(id));
    }

    @Test
    void testAddTagsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.addTagsToResource(id, Sets.newHashSet(), Command.class));
    }

    @Test
    void testUpdateTagsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.updateTagsForResource(id, Sets.newHashSet(), Command.class));
    }

    @Test
    void testGetTagsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getTagsForResource(id, Command.class));
    }

    @Test
    void testRemoveAllTagsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeAllTagsForResource(id, Command.class));
    }

    @Test
    void testRemoveTagForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeTagForResource(id, "something", Command.class));
    }

    @Test
    void testGetClustersForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getClustersForCommand(id, null));
    }
}
