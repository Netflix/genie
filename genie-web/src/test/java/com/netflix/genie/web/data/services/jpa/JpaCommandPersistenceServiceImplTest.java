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
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.CommandMetadata;
import com.netflix.genie.common.external.dtos.v4.CommandRequest;
import com.netflix.genie.common.external.dtos.v4.CommandStatus;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.web.data.entities.ApplicationEntity;
import com.netflix.genie.web.data.entities.CommandEntity;
import com.netflix.genie.web.data.repositories.jpa.JpaApplicationRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaClusterRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCommandRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCriterionRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Tests for the CommandServiceJPAImpl.
 *
 * @author tgianos
 * @since 2.0.0
 */
class JpaCommandPersistenceServiceImplTest {

    private static final String COMMAND_1_ID = "command1";
    private static final String COMMAND_1_NAME = "pig_13_prod";
    private static final String COMMAND_1_USER = "tgianos";
    private static final String COMMAND_1_VERSION = "1.2.3";
    private static final List<String> COMMAND_1_EXECUTABLE = Lists.newArrayList("pig");
    private static final long COMMAND_1_CHECK_DELAY = 18000L;

    private static final String COMMAND_2_ID = "command2";

    private JpaCommandPersistenceServiceImpl service;
    private JpaCommandRepository jpaCommandRepository;
    private JpaApplicationRepository jpaApplicationRepository;

    /**
     * Setup the tests.
     */
    @BeforeEach
    void setup() {
        this.jpaCommandRepository = Mockito.mock(JpaCommandRepository.class);
        this.jpaApplicationRepository = Mockito.mock(JpaApplicationRepository.class);
        this.service = new JpaCommandPersistenceServiceImpl(
            Mockito.mock(JpaTagPersistenceService.class),
            Mockito.mock(JpaFilePersistenceService.class),
            this.jpaApplicationRepository,
            Mockito.mock(JpaClusterRepository.class),
            this.jpaCommandRepository,
            Mockito.mock(JpaCriterionRepository.class)
        );
    }

    /**
     * Test the get command method.
     */
    @Test
    void testGetCommandNotExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getCommand(id));
    }

    /**
     * Test to make sure an exception is thrown when command already exists.
     */
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
            .assertThatExceptionOfType(GenieConflictException.class)
            .isThrownBy(() -> this.service.createCommand(command));
    }

    /**
     * Test to update an command.
     */
    @Test
    void testUpdateCommandNoCommandExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
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

    /**
     * Test to update an command.
     */
    @Test
    void testUpdateCommandIdsDontMatch() {
        Mockito.when(this.jpaCommandRepository.existsByUniqueId(COMMAND_2_ID)).thenReturn(true);
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(UUID.randomUUID().toString());
        Assertions
            .assertThatExceptionOfType(GenieBadRequestException.class)
            .isThrownBy(() -> this.service.updateCommand(COMMAND_2_ID, command));
    }

    /**
     * Test delete.
     */
    @Test
    void testDeleteNoCommandToDelete() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.deleteCommand(id));
    }

    /**
     * Test add configurations to command.
     */
    @Test
    void testAddConfigsToCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.addConfigsForCommand(id, Sets.newHashSet()));
    }

    /**
     * Test update configurations for command.
     */
    @Test
    void testUpdateConfigsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.updateConfigsForCommand(id, Sets.newHashSet()));
    }

    /**
     * Test get configurations to command.
     */
    @Test
    void testGetConfigsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getConfigsForCommand(id));
    }

    /**
     * Test remove all configurations for command.
     */
    @Test
    void testRemoveAllConfigsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.removeAllConfigsForCommand(id));
    }

    /**
     * Test remove configuration for command.
     */
    @Test
    void testRemoveConfigForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.removeConfigForCommand(id, "something"));
    }

    /**
     * Test add dependencies to command.
     */
    @Test
    void testAddDependenciesForCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.addDependenciesForCommand(id, Sets.newHashSet()));
    }

    /**
     * Test update dependencies for command.
     */
    @Test
    void testUpdateDependenciesForCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.updateDependenciesForCommand(id, Sets.newHashSet()));
    }

    /**
     * Test get dependencies to command.
     */
    @Test
    void testGetDependenciesForCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getDependenciesForCommand(id));
    }

    /**
     * Test remove all dependencies for command.
     */
    @Test
    void testRemoveAllDependenciesForCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.removeAllDependenciesForCommand(id));
    }

    /**
     * Test remove configuration for command.
     */
    @Test
    void testRemoveDependencyForCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.removeDependencyForCommand(id, "something"));
    }

    /**
     * Make sure we can't add duplicate applications to the cluster.
     */
    @Test
    void cantAddDuplicateApplicationsToCommand() {
        final String applicationId1 = UUID.randomUUID().toString();
        final String applicationId2 = UUID.randomUUID().toString();
        final List<String> applicationIds = Lists.newArrayList(applicationId2);
        final CommandEntity commandEntity = Mockito.mock(CommandEntity.class);
        final ApplicationEntity app1 = Mockito.mock(ApplicationEntity.class);
        Mockito.when(app1.getUniqueId()).thenReturn(applicationId1);
        final ApplicationEntity app2 = Mockito.mock(ApplicationEntity.class);
        Mockito.when(app2.getUniqueId()).thenReturn(applicationId2);
        Mockito.when(commandEntity.getApplications()).thenReturn(Lists.newArrayList(app1, app2));

        Mockito.when(this.jpaCommandRepository.findByUniqueId(COMMAND_1_ID)).thenReturn(Optional.of(commandEntity));
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(applicationId1)).thenReturn(Optional.of(app1));
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(applicationId2)).thenReturn(Optional.of(app2));
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.service.addApplicationsForCommand(COMMAND_1_ID, applicationIds));
    }

    /**
     * Test setting the applications for a given command.
     */
    @Test
    void testSetApplicationsForCommandNoAppId() {
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(
                () -> this.service.setApplicationsForCommand(
                    COMMAND_2_ID,
                    Lists.newArrayList(UUID.randomUUID().toString())
                )
            );
    }

    /**
     * Test setting the applications for a given command.
     */
    @Test
    void testSetApplicationsForCommandNoCommandExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Mockito.when(this.jpaApplicationRepository.existsByUniqueId(Mockito.anyString())).thenReturn(true);
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.service.setApplicationsForCommand(id, Lists.newArrayList(UUID.randomUUID().toString()))
            );
    }

    /**
     * Test setting the applications for a given command.
     */
    @Test
    void testSetApplicationsForCommandNoAppExists() {
        final String appId = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.existsByUniqueId(appId)).thenReturn(false);
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.service.setApplicationsForCommand(COMMAND_2_ID, Lists.newArrayList(appId)));
    }

    /**
     * Make sure we can't update the applications for a command if there are duplicate ids in the list of appIds.
     */
    @Test
    void cantUpdateApplicationsForCommandWithDuplicates() {
        final String appId1 = UUID.randomUUID().toString();
        final String appId2 = UUID.randomUUID().toString();
        final List<String> appIds = Lists.newArrayList(appId1, appId2, appId1);
        final CommandEntity command = Mockito.mock(CommandEntity.class);
        Mockito.when(this.jpaCommandRepository.findByUniqueId(COMMAND_1_ID)).thenReturn(Optional.of(command));
        Mockito
            .when(this.jpaApplicationRepository.findByUniqueId(appId1))
            .thenReturn(Optional.of(Mockito.mock(ApplicationEntity.class)));
        Mockito
            .when(this.jpaApplicationRepository.findByUniqueId(appId2))
            .thenReturn(Optional.of(Mockito.mock(ApplicationEntity.class)));
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.service.setApplicationsForCommand(COMMAND_1_ID, appIds));
    }

    /**
     * Test get applications for command.
     */
    @Test
    void testGetApplicationsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getApplicationsForCommand(id));
    }

    /**
     * Test remove applications for command.
     */
    @Test
    void testRemoveApplicationsForCommandNoCommandExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.removeApplicationsForCommand(id));
    }

    /**
     * Test add tags to command.
     */
    @Test
    void testAddTagsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.addTagsForCommand(id, Sets.newHashSet()));
    }

    /**
     * Test update tags for command.
     */
    @Test
    void testUpdateTagsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.updateTagsForCommand(id, Sets.newHashSet()));
    }

    /**
     * Test get tags to command.
     */
    @Test
    void testGetTagsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getTagsForCommand(id));
    }

    /**
     * Test remove all tags for command.
     */
    @Test
    void testRemoveAllTagsForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.removeAllTagsForCommand(id));
    }

    /**
     * Test remove configuration for command.
     */
    @Test
    void testRemoveTagForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.removeTagForCommand(id, "something"));
    }

    /**
     * Test the Get clusters for command function.
     */
    @Test
    void testGetClustersForCommandNoCommand() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getClustersForCommand(id, null));
    }
}
