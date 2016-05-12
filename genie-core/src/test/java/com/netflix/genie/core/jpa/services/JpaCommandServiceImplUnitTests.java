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
package com.netflix.genie.core.jpa.services;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.jpa.entities.ApplicationEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;

/**
 * Tests for the CommandServiceJPAImpl.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(UnitTest.class)
public class JpaCommandServiceImplUnitTests {

    private static final String COMMAND_1_ID = "command1";
    private static final String COMMAND_1_NAME = "pig_13_prod";
    private static final String COMMAND_1_USER = "tgianos";
    private static final String COMMAND_1_VERSION = "1.2.3";
    private static final String COMMAND_1_EXECUTABLE = "pig";
    private static final long COMMAND_1_CHECK_DELAY = 18000L;

    private static final String COMMAND_2_ID = "command2";

    private JpaCommandServiceImpl service;
    private JpaCommandRepository jpaCommandRepository;
    private JpaApplicationRepository jpaApplicationRepository;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.jpaCommandRepository = Mockito.mock(JpaCommandRepository.class);
        final JpaClusterRepository jpaClusterRepository = Mockito.mock(JpaClusterRepository.class);
        this.jpaApplicationRepository = Mockito.mock(JpaApplicationRepository.class);
        this.service = new JpaCommandServiceImpl(
            this.jpaCommandRepository,
            this.jpaApplicationRepository,
            jpaClusterRepository
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
        this.service.getCommand(id);
    }

    /**
     * Test to make sure an exception is thrown when command already exists.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateCommandAlreadyExists() throws GenieException {
        final Command command = new Command.Builder(
            COMMAND_1_NAME,
            COMMAND_1_USER,
            COMMAND_1_VERSION,
            CommandStatus.ACTIVE,
            COMMAND_1_EXECUTABLE,
            COMMAND_1_CHECK_DELAY
        )
            .withId(COMMAND_1_ID)
            .build();
        Mockito.when(this.jpaCommandRepository.exists(COMMAND_1_ID)).thenReturn(true);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
        this.service.updateCommand(
            id,
            new Command.Builder(null, null, null, null, null, 1803L).build()
        );
    }

    /**
     * Test to update an command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieBadRequestException.class)
    public void testUpdateCommandIdsDontMatch() throws GenieException {
        Mockito.when(this.jpaCommandRepository.exists(COMMAND_2_ID)).thenReturn(true);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
        this.service.addConfigsForCommand(id, new HashSet<>());
    }

    /**
     * Test update configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateConfigsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
        this.service.updateConfigsForCommand(UUID.randomUUID().toString(), new HashSet<>());
    }

    /**
     * Test get configurations to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetConfigsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
        this.service.removeConfigForCommand(id, "something");
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
        Mockito.when(app1.getId()).thenReturn(applicationId1);
        final ApplicationEntity app2 = Mockito.mock(ApplicationEntity.class);
        Mockito.when(app2.getId()).thenReturn(applicationId2);
        Mockito.when(commandEntity.getApplications()).thenReturn(Lists.newArrayList(app1, app2));

        Mockito.when(this.jpaCommandRepository.findOne(COMMAND_1_ID)).thenReturn(commandEntity);
        Mockito.when(this.jpaApplicationRepository.findOne(applicationId1)).thenReturn(app1);
        Mockito.when(this.jpaApplicationRepository.findOne(applicationId2)).thenReturn(app2);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
        Mockito.when(this.jpaApplicationRepository.exists(Mockito.anyString())).thenReturn(true);
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
        Mockito.when(this.jpaApplicationRepository.exists(appId)).thenReturn(false);
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
        Mockito.when(this.jpaCommandRepository.findOne(COMMAND_1_ID)).thenReturn(command);
        Mockito.when(this.jpaApplicationRepository.findOne(appId1)).thenReturn(Mockito.mock(ApplicationEntity.class));
        Mockito.when(this.jpaApplicationRepository.findOne(appId2)).thenReturn(Mockito.mock(ApplicationEntity.class));
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
        this.service.addTagsForCommand(id, new HashSet<>());
    }

    /**
     * Test update tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateTagsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
        this.service.updateTagsForCommand(UUID.randomUUID().toString(), new HashSet<>());
    }

    /**
     * Test get tags to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetTagsForCommandNoCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
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
        Mockito.when(this.jpaCommandRepository.findOne(id)).thenReturn(null);
        this.service.getClustersForCommand(id, null);
    }
}
