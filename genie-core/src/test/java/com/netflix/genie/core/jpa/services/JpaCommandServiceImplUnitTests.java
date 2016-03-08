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
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.UUID;

/**
 * Tests for the CommandServiceJPAImpl.
 *
 * @author tgianos
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
    @Test
    @Ignore
    public void testGetCommand() throws GenieException {
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
     * Test the create method.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testCreateCommand() throws GenieException {
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
    @Test
    @Ignore
    public void testUpdateCommand() throws GenieException {
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
     * Test delete all.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testDeleteAll() throws GenieException {
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testDelete() throws GenieException {
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
    @Test
    @Ignore
    public void testAddConfigsToCommand() throws GenieException {
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
    @Test
    @Ignore
    public void testUpdateConfigsForCommand() throws GenieException {
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
     * Test get configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testGetConfigsForCommand() throws GenieException {
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
    @Test
    @Ignore
    public void testRemoveAllConfigsForCommand() throws GenieException {
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
    @Test
    @Ignore
    public void testRemoveConfigForCommand() throws GenieException {
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
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testSetApplicationsForCommand() throws GenieException {
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
     * Test get applications for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testGetApplicationsForCommand() throws GenieException {
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
    @Test
    @Ignore
    public void testRemoveApplicationsForCommand() throws GenieException {
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
    @Test
    @Ignore
    public void testAddTagsToCommand() throws GenieException {
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
    @Test
    @Ignore
    public void testUpdateTagsForCommand() throws GenieException {
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
     * Test get tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testGetTagsForCommand() throws GenieException {
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
    @Test
    @Ignore
    public void testRemoveAllTagsForCommand() throws GenieException {
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
     * Test remove tag for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testRemoveTagForCommand() throws GenieException {
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
    @Test
    @Ignore
    public void testGetCommandsForCommand() throws GenieException {
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
