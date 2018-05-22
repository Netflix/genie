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
package com.netflix.genie.web.jpa.services;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.CommandMetadata;
import com.netflix.genie.common.internal.dto.v4.CommandRequest;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
@Category(UnitTest.class)
public class JpaCommandPersistenceServiceImplUnitTests {

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
    @Before
    public void setup() {
        this.jpaCommandRepository = Mockito.mock(JpaCommandRepository.class);
        this.jpaApplicationRepository = Mockito.mock(JpaApplicationRepository.class);
        this.service = new JpaCommandPersistenceServiceImpl(
            Mockito.mock(JpaTagPersistenceService.class),
            Mockito.mock(JpaFilePersistenceService.class),
            this.jpaApplicationRepository,
            Mockito.mock(JpaClusterRepository.class),
            this.jpaCommandRepository
        );
    }

    /**
     * Test the get command method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetCommandNotExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.getCommand(id);
    }

    /**
     * Test to make sure an exception is thrown when command already exists.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateCommandAlreadyExists() throws GenieException {
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
        this.service.createCommand(command);
    }

    /**
     * Test to update an command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateCommandNoCommandExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.updateCommand(
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
                1803L
            )
        );
    }

    /**
     * Test to update an command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieBadRequestException.class)
    public void testUpdateCommandIdsDontMatch() throws GenieException {
        Mockito.when(this.jpaCommandRepository.existsByUniqueId(COMMAND_2_ID)).thenReturn(true);
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(UUID.randomUUID().toString());
        this.service.updateCommand(COMMAND_2_ID, command);
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testDeleteNoCommandToDelete() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.deleteCommand(id);
    }

    /**
     * Test add configurations to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddConfigsToCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.addConfigsForCommand(id, Sets.newHashSet());
    }

    /**
     * Test update configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateConfigsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.updateConfigsForCommand(id, Sets.newHashSet());
    }

    /**
     * Test get configurations to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetConfigsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.getConfigsForCommand(id);
    }

    /**
     * Test remove all configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllConfigsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.removeAllConfigsForCommand(id);
    }

    /**
     * Test remove configuration for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveConfigForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.removeConfigForCommand(id, "something");
    }

    /**
     * Test add dependencies to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddDependenciesForCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.addDependenciesForCommand(id, Sets.newHashSet());
    }

    /**
     * Test update dependencies for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateDependenciesForCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.updateDependenciesForCommand(id, Sets.newHashSet());
    }

    /**
     * Test get dependencies to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetDependenciesForCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.getDependenciesForCommand(id);
    }

    /**
     * Test remove all dependencies for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllDependenciesForCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.removeAllDependenciesForCommand(id);
    }

    /**
     * Test remove configuration for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveJarForCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.removeDependencyForCommand(id, "something");
    }

    /**
     * Make sure we can't add duplicate applications to the cluster.
     *
     * @throws GenieException on error
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantAddDuplicateApplicationsToCommand() throws GenieException {
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
        this.service.addApplicationsForCommand(COMMAND_1_ID, applicationIds);
    }

    /**
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetApplicationsForCommandNoAppId() throws GenieException {
        this.service.setApplicationsForCommand(COMMAND_2_ID, Lists.newArrayList(UUID.randomUUID().toString()));
    }

    /**
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetApplicationsForCommandNoCommandExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Mockito.when(this.jpaApplicationRepository.existsByUniqueId(Mockito.anyString())).thenReturn(true);
        this.service.setApplicationsForCommand(id, Lists.newArrayList(UUID.randomUUID().toString()));
    }

    /**
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetApplicationsForCommandNoAppExists() throws GenieException {
        final String appId = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.existsByUniqueId(appId)).thenReturn(false);
        this.service.setApplicationsForCommand(COMMAND_2_ID, Lists.newArrayList(appId));
    }

    /**
     * Make sure we can't update the applications for a command if there are duplicate ids in the list of appIds.
     *
     * @throws GenieException On error
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantUpdateApplicationsForCommandWithDuplicates() throws GenieException {
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
        this.service.setApplicationsForCommand(COMMAND_1_ID, appIds);
    }

    /**
     * Test get applications for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetApplicationsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.getApplicationsForCommand(id);
    }

    /**
     * Test remove applications for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveApplicationsForCommandNoCommandExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.removeApplicationsForCommand(id);
    }

    /**
     * Test add tags to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddTagsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.addTagsForCommand(id, Sets.newHashSet());
    }

    /**
     * Test update tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateTagsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.updateTagsForCommand(id, Sets.newHashSet());
    }

    /**
     * Test get tags to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetTagsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.getTagsForCommand(id);
    }

    /**
     * Test remove all tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllTagsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.removeAllTagsForCommand(id);
    }

    /**
     * Test remove configuration for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveTagForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.removeTagForCommand(id, "something");
    }

    /**
     * Test the Get clusters for command function.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetClustersForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.getClustersForCommand(id, null);
    }
}
