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
package com.netflix.genie.core.services.impl.jpa;

import com.google.common.collect.Lists;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.ApplicationStatus;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.core.repositories.jpa.ApplicationRepository;
import com.netflix.genie.core.repositories.jpa.CommandRepository;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;

import java.util.HashSet;
import java.util.UUID;

/**
 * Tests for the ApplicationConfigServiceJPAImpl.
 *
 * @author tgianos
 */
public class TestApplicationConfigServiceJPAImpl {

    private static final String APP_1_ID = "app1";
    private static final String APP_1_NAME = "tez";
    private static final String APP_1_USER = "tgianos";
    private static final String APP_1_VERSION = "1.2.3";

    private com.netflix.genie.core.repositories.jpa.ApplicationRepository applicationRepository;
    private ApplicationConfigServiceJPAImpl appService;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.applicationRepository = Mockito.mock(ApplicationRepository.class);
        final CommandRepository commandRepository = Mockito.mock(CommandRepository.class);
        this.appService = new ApplicationConfigServiceJPAImpl(this.applicationRepository, commandRepository);
    }

    /**
     * Test the get application method.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testGetApplication() throws GenieException {
    }

    /**
     * Test the get application method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetApplicationNotExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(Mockito.eq(id))).thenReturn(null);
        this.appService.getApplication(UUID.randomUUID().toString());
    }

    /**
     * Test the create method.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testCreateApplication() throws GenieException {
    }

    /**
     * Test the create method when no id is entered.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testCreateApplicationNoId() throws GenieException {
    }

    /**
     * Test to make sure an exception is thrown when application already exists.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateApplicationAlreadyExists() throws GenieException {
        final Application app = new Application(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
        );
        app.setId(APP_1_ID);
        Mockito.when(this.applicationRepository.exists(Mockito.eq(APP_1_ID))).thenReturn(true);
        this.appService.createApplication(app);
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testUpdateApplication() throws GenieException {
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateApplicationNoAppExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(Mockito.eq(id))).thenReturn(null);
        this.appService.updateApplication(id, new Application());
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieBadRequestException.class)
    public void testUpdateApplicationIdsDontMatch() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final Application updateApp = new Application();
        updateApp.setId(UUID.randomUUID().toString());
        Mockito.when(this.applicationRepository.exists(id)).thenReturn(true);
        this.appService.updateApplication(id, updateApp);
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
     * Test delete all when still in a relationship with a command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testDeleteAllBlocked() throws GenieException {
        final Application application = Mockito.mock(Application.class);
        final Command command = Mockito.mock(Command.class);
        Mockito.when(this.applicationRepository.findAll()).thenReturn(Lists.newArrayList(application));
        Mockito.when(application.getCommands()).thenReturn(Sets.newSet(command));
        this.appService.deleteAllApplications();
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
     * Test to make sure delete is blocked if the application is still linked to a command.
     *
     * @throws GenieException Due to the application still being linked to a command in the database
     */
    @Test(expected = GeniePreconditionException.class)
    @Ignore
    public void testDeleteBlocked() throws GenieException {
        final Application application = Mockito.mock(Application.class);
        final String id = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(this.applicationRepository.findOne(Mockito.eq(id))).thenReturn(application);
        Mockito.when(application.getCommands()).thenReturn(Sets.newSet(command));
        this.appService.deleteApplication(id);
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieException.class)
    public void testDeleteNoAppToDelete() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(Mockito.eq(id))).thenReturn(null);
        this.appService.deleteApplication(id);
    }

    /**
     * Test add configurations to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddConfigsToApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.getOne(Mockito.eq(id))).thenReturn(null);
        this.appService.addConfigsToApplication(id, new HashSet<>());
    }

    /**
     * Test update configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testUpdateConfigsForApplication() throws GenieException {
    }

    /**
     * Test update configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateConfigsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(Mockito.eq(id))).thenReturn(null);
        this.appService.updateConfigsForApplication(id, new HashSet<>());
    }

    /**
     * Test get configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testGetConfigsForApplication() throws GenieException {
    }

    /**
     * Test get configurations to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetConfigsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.getConfigsForApplication(id);
    }

    /**
     * Test remove all configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testRemoveAllConfigsForApplication() throws GenieException {
    }

    /**
     * Test remove all configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllConfigsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.removeAllConfigsForApplication(id);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testRemoveConfigForApplication() throws GenieException {
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveConfigForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.removeConfigForApplication(id, "something");
    }

    /**
     * Test add jars to application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testAddJarsToApplication() throws GenieException {
    }

    /**
     * Test add jars to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddJarsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.addJarsForApplication(id, new HashSet<>());
    }

    /**
     * Test update jars for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testUpdateJarsForApplication() throws GenieException {
    }

    /**
     * Test update jars for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateJarsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.updateJarsForApplication(id, new HashSet<>());
    }

    /**
     * Test get jars for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testGetJarsForApplication() throws GenieException {
    }

    /**
     * Test get jars to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetJarsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.getJarsForApplication(id);
    }

    /**
     * Test remove all jars for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testRemoveAllJarsForApplication() throws GenieException {
    }

    /**
     * Test remove all jars for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllJarsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.removeAllJarsForApplication(id);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testRemoveJarForApplication() throws GenieException {
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveJarForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.removeJarForApplication(id, "something");
    }

    /**
     * Test add tags to application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testAddTagsToApplication() throws GenieException {
    }

    /**
     * Test add tags to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddTagsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.addTagsForApplication(id, new HashSet<>());
    }

    /**
     * Test update tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testUpdateTagsForApplication() throws GenieException {
    }

    /**
     * Test update tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateTagsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.updateTagsForApplication(id, new HashSet<>());
    }

    /**
     * Test get tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testGetTagsForApplication() throws GenieException {
    }

    /**
     * Test get tags to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetTagsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.getTagsForApplication(id);
    }

    /**
     * Test remove all tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testRemoveAllTagsForApplication() throws GenieException {
    }

    /**
     * Test remove all tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllTagsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.removeAllTagsForApplication(id);
    }

    /**
     * Test remove tag for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testRemoveTagForApplication() throws GenieException {
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveTagForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.removeTagForApplication(id, "something");
    }

    /**
     * Test the Get commands for application method.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testGetCommandsForApplication() throws GenieException {
    }

    /**
     * Test the Get commands for application method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetCommandsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.applicationRepository.findOne(id)).thenReturn(null);
        this.appService.getCommandsForApplication(id, null);
    }
}
