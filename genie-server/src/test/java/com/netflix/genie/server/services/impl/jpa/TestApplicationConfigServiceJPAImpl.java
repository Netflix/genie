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
package com.netflix.genie.server.services.impl.jpa;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.ApplicationStatus;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.server.services.ApplicationConfigService;
import com.netflix.genie.server.services.CommandConfigService;
import java.net.HttpURLConnection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import javax.validation.ConstraintViolationException;

/**
 * Tests for the ApplicationConfigServiceJPAImpl. Really these are integration tests.
 *
 * @author tgianos
 */
@DatabaseSetup("application/init.xml")
public class TestApplicationConfigServiceJPAImpl extends DBUnitTestBase {

    private static final String APP_1_ID = "app1";
    private static final String APP_1_NAME = "tez";
    private static final String APP_1_USER = "tgianos";
    private static final String APP_1_VERSION = "1.2.3";
    private static final ApplicationStatus APP_1_STATUS = ApplicationStatus.INACTIVE;

    private static final String COMMAND_1_ID = "command1";

    private static final String APP_2_ID = "app2";
    private static final String APP_2_NAME = "spark";
    private static final String APP_2_USER = "amsharma";
    private static final String APP_2_VERSION = "4.5.6";
    private static final ApplicationStatus APP_2_STATUS = ApplicationStatus.ACTIVE;

    private static final String APP_3_ID = "app3";
    private static final String APP_3_NAME = "storm";
    private static final String APP_3_USER = "tgianos";
    private static final String APP_3_VERSION = "7.8.9";
    private static final ApplicationStatus APP_3_STATUS = ApplicationStatus.DEPRECATED;

    @Inject
    private ApplicationConfigService service;

    @Inject
    private CommandConfigService commandService;

    /**
     * Test the get application method.
     *
     * @throws GenieException
     */
    @Test
    public void testGetApplication() throws GenieException {
        final Application app = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_ID, app.getId());
        Assert.assertEquals(APP_1_NAME, app.getName());
        Assert.assertEquals(APP_1_USER, app.getUser());
        Assert.assertEquals(APP_1_VERSION, app.getVersion());
        Assert.assertEquals(APP_1_STATUS, app.getStatus());
        Assert.assertEquals(3, app.getTags().size());
        Assert.assertEquals(2, app.getConfigs().size());
        Assert.assertEquals(2, app.getJars().size());

        final Application app2 = this.service.getApplication(APP_2_ID);
        Assert.assertEquals(APP_2_ID, app2.getId());
        Assert.assertEquals(APP_2_NAME, app2.getName());
        Assert.assertEquals(APP_2_USER, app2.getUser());
        Assert.assertEquals(APP_2_VERSION, app2.getVersion());
        Assert.assertEquals(APP_2_STATUS, app2.getStatus());
        Assert.assertEquals(4, app2.getTags().size());
        Assert.assertEquals(2, app2.getConfigs().size());
        Assert.assertEquals(1, app2.getJars().size());

        final Application app3 = this.service.getApplication(APP_3_ID);
        Assert.assertEquals(APP_3_ID, app3.getId());
        Assert.assertEquals(APP_3_NAME, app3.getName());
        Assert.assertEquals(APP_3_USER, app3.getUser());
        Assert.assertEquals(APP_3_VERSION, app3.getVersion());
        Assert.assertEquals(APP_3_STATUS, app3.getStatus());
        Assert.assertEquals(3, app3.getTags().size());
        Assert.assertEquals(1, app3.getConfigs().size());
        Assert.assertEquals(2, app3.getJars().size());
    }

    /**
     * Test the get application method.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetApplicationNull() throws GenieException {
        this.service.getApplication(null);
    }

    /**
     * Test the get application method.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetApplicationNotExists() throws GenieException {
        this.service.getApplication(UUID.randomUUID().toString());
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByName() {
        final List<Application> apps = this.service.getApplications(
                APP_2_NAME, null, null, null, 0, 10, true, null);
        Assert.assertEquals(1, apps.size());
        Assert.assertEquals(APP_2_ID, apps.get(0).getId());
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByUserName() {
        final List<Application> apps = this.service.getApplications(
                null, APP_1_USER, null, null, -1, -5000, true, null);
        Assert.assertEquals(2, apps.size());
        Assert.assertEquals(APP_3_ID, apps.get(0).getId());
        Assert.assertEquals(APP_1_ID, apps.get(1).getId());
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByStatuses() {
        final Set<ApplicationStatus> statuses = new HashSet<>();
        statuses.add(ApplicationStatus.ACTIVE);
        statuses.add(ApplicationStatus.INACTIVE);
        final List<Application> apps = this.service.getApplications(
                null, null, statuses, null, -1, -5000, true, null);
        Assert.assertEquals(2, apps.size());
        Assert.assertEquals(APP_2_ID, apps.get(0).getId());
        Assert.assertEquals(APP_1_ID, apps.get(1).getId());
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByTags() {
        final Set<String> tags = new HashSet<>();
        tags.add("prod");
        List<Application> apps = this.service.getApplications(
                null, null, null, tags, 0, 10, true, null);
        Assert.assertEquals(3, apps.size());
        Assert.assertEquals(APP_3_ID, apps.get(0).getId());
        Assert.assertEquals(APP_2_ID, apps.get(1).getId());
        Assert.assertEquals(APP_1_ID, apps.get(2).getId());

        tags.add("yarn");
        apps = this.service.getApplications(
                null, null, null, tags, 0, 10, true, null);
        Assert.assertEquals(1, apps.size());
        Assert.assertEquals(APP_2_ID, apps.get(0).getId());

        tags.clear();
        tags.add("genie.name:spark");
        apps = this.service.getApplications(
                null, null, null, tags, 0, 10, true, null);
        Assert.assertEquals(1, apps.size());
        Assert.assertEquals(APP_2_ID, apps.get(0).getId());

        tags.add("somethingThatWouldNeverReallyExist");
        apps = this.service.getApplications(
                null, null, null, tags, 0, 10, true, null);
        Assert.assertTrue(apps.isEmpty());

        tags.clear();
        apps = this.service.getApplications(
                null, null, null, tags, 0, 10, true, null);
        Assert.assertEquals(3, apps.size());
        Assert.assertEquals(APP_3_ID, apps.get(0).getId());
        Assert.assertEquals(APP_2_ID, apps.get(1).getId());
        Assert.assertEquals(APP_1_ID, apps.get(2).getId());
    }

    /**
     * Test the get applications method with descending sort.
     */
    @Test
    public void testGetClustersDescending() {
        //Default to order by Updated
        final List<Application> applications = this.service.getApplications(null, null, null, null, 0, 10, true, null);
        Assert.assertEquals(3, applications.size());
        Assert.assertEquals(APP_3_ID, applications.get(0).getId());
        Assert.assertEquals(APP_2_ID, applications.get(1).getId());
        Assert.assertEquals(APP_1_ID, applications.get(2).getId());
    }

    /**
     * Test the get applications method with ascending sort.
     */
    @Test
    public void testGetClustersAscending() {
        //Default to order by Updated
        final List<Application> applications = this.service.getApplications(null, null, null, null, 0, 10, false, null);
        Assert.assertEquals(3, applications.size());
        Assert.assertEquals(APP_1_ID, applications.get(0).getId());
        Assert.assertEquals(APP_2_ID, applications.get(1).getId());
        Assert.assertEquals(APP_3_ID, applications.get(2).getId());
    }

    /**
     * Test the get applications method default order by.
     */
    @Test
    public void testGetClustersOrderBysDefault() {
        //Default to order by Updated
        final List<Application> applications = this.service.getApplications(null, null, null, null, 0, 10, true, null);
        Assert.assertEquals(3, applications.size());
        Assert.assertEquals(APP_3_ID, applications.get(0).getId());
        Assert.assertEquals(APP_2_ID, applications.get(1).getId());
        Assert.assertEquals(APP_1_ID, applications.get(2).getId());
    }

    /**
     * Test the get applications method order by updated.
     */
    @Test
    public void testGetClustersOrderBysUpdated() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("updated");
        final List<Application> applications =
                this.service.getApplications(null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(3, applications.size());
        Assert.assertEquals(APP_3_ID, applications.get(0).getId());
        Assert.assertEquals(APP_2_ID, applications.get(1).getId());
        Assert.assertEquals(APP_1_ID, applications.get(2).getId());
    }

    /**
     * Test the get applications method order by name.
     */
    @Test
    public void testGetClustersOrderBysName() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("name");
        final List<Application> applications =
                this.service.getApplications(null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(3, applications.size());
        Assert.assertEquals(APP_1_ID, applications.get(0).getId());
        Assert.assertEquals(APP_3_ID, applications.get(1).getId());
        Assert.assertEquals(APP_2_ID, applications.get(2).getId());
    }

    /**
     * Test the get applications method order by an invalid field should return the order by default value (updated).
     */
    @Test
    public void testGetClustersOrderBysInvalidField() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("I'mNotAValidField");
        final List<Application> applications =
                this.service.getApplications(null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(3, applications.size());
        Assert.assertEquals(APP_3_ID, applications.get(0).getId());
        Assert.assertEquals(APP_2_ID, applications.get(1).getId());
        Assert.assertEquals(APP_1_ID, applications.get(2).getId());
    }

    /**
     * Test the get applications method order by a collection field should return the order by default value (updated).
     */
    @Test
    public void testGetClustersOrderBysCollectionField() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("tags");
        final List<Application> applications =
                this.service.getApplications(null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(3, applications.size());
        Assert.assertEquals(APP_3_ID, applications.get(0).getId());
        Assert.assertEquals(APP_2_ID, applications.get(1).getId());
        Assert.assertEquals(APP_1_ID, applications.get(2).getId());
    }

    /**
     * Test the create method.
     *
     * @throws GenieException
     */
    @Test
    public void testCreateApplication() throws GenieException {
        final Application app = new Application(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
        );
        final String id = UUID.randomUUID().toString();
        app.setId(id);
        final Application created = this.service.createApplication(app);
        Assert.assertNotNull(this.service.getApplication(id));
        Assert.assertEquals(id, created.getId());
        Assert.assertEquals(APP_1_NAME, created.getName());
        Assert.assertEquals(APP_1_USER, created.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, created.getStatus());
        this.service.deleteApplication(id);
        try {
            this.service.getApplication(id);
            Assert.fail("Should have thrown exception");
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    ge.getErrorCode()
            );
        }
    }

    /**
     * Test the create method when no id is entered.
     *
     * @throws GenieException
     */
    @Test
    public void testCreateApplicationNoId() throws GenieException {
        final Application app = new Application(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
        );
        final Application created = this.service.createApplication(app);
        Assert.assertNotNull(this.service.getApplication(created.getId()));
        Assert.assertEquals(APP_1_NAME, created.getName());
        Assert.assertEquals(APP_1_USER, created.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, created.getStatus());
        this.service.deleteApplication(created.getId());
        try {
            this.service.getApplication(created.getId());
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    ge.getErrorCode()
            );
        }
    }

    /**
     * Test to make sure an exception is thrown when null is entered.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testCreateApplicationNull() throws GenieException {
        this.service.createApplication(null);
    }

    /**
     * Test to make sure an exception is thrown when application already exists.
     *
     * @throws GenieException
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
        this.service.createApplication(app);
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateApplicationNoId() throws GenieException {
        final Application init = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_USER, init.getUser());
        Assert.assertEquals(ApplicationStatus.INACTIVE, init.getStatus());
        Assert.assertEquals(3, init.getTags().size());

        final Application updateApp = new Application();
        updateApp.setStatus(ApplicationStatus.ACTIVE);
        updateApp.setUser(APP_2_USER);
        final Set<String> tags = new HashSet<>();
        tags.add("prod");
        tags.add("tez");
        tags.add("yarn");
        tags.add("hadoop");
        updateApp.setTags(tags);
        this.service.updateApplication(APP_1_ID, updateApp);

        final Application updated = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_2_USER, updated.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, updated.getStatus());
        Assert.assertEquals(6, updated.getTags().size());
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateApplicationWithId() throws GenieException {
        final Application init = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_USER, init.getUser());
        Assert.assertEquals(ApplicationStatus.INACTIVE, init.getStatus());
        Assert.assertEquals(3, init.getTags().size());

        final Application updateApp = new Application();
        updateApp.setId(APP_1_ID);
        updateApp.setStatus(ApplicationStatus.ACTIVE);
        updateApp.setUser(APP_2_USER);
        final Set<String> tags = new HashSet<>();
        tags.add("prod");
        tags.add("tez");
        tags.add("yarn");
        tags.add("hadoop");
        updateApp.setTags(tags);
        this.service.updateApplication(APP_1_ID, updateApp);

        final Application updated = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_2_USER, updated.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, updated.getStatus());
        Assert.assertEquals(6, updated.getTags().size());
    }

    /**
     * Test to update an application that makes it invalid. Should throw exception.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateApplicationNotValid() throws GenieException {
        final Application init = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_USER, init.getUser());
        Assert.assertEquals(ApplicationStatus.INACTIVE, init.getStatus());
        Assert.assertEquals(3, init.getTags().size());

        final Application updateApp = new Application();
        updateApp.setId(APP_1_ID);
        updateApp.setStatus(ApplicationStatus.ACTIVE);
        updateApp.setUser("");
        this.service.updateApplication(APP_1_ID, updateApp);
    }

    /**
     * Test to make sure setting the created and updated outside the system control doesn't change record in database.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateCreateAndUpdate() throws GenieException {
        final Application init = this.service.getApplication(APP_1_ID);
        final Date created = init.getCreated();
        final Date updated = init.getUpdated();

        init.setCreated(new Date());
        final Date zero = new Date(0);
        init.setUpdated(zero);

        final Application updatedApp = this.service.updateApplication(APP_1_ID, init);
        Assert.assertEquals(created, updatedApp.getCreated());
        Assert.assertNotEquals(updated, updatedApp.getUpdated());
        Assert.assertNotEquals(zero, updatedApp.getUpdated());
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateApplicationNullId() throws GenieException {
        this.service.updateApplication(null, new Application());
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateApplicationNullUpdateApp() throws GenieException {
        this.service.updateApplication(APP_1_ID, null);
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateApplicationNoAppExists() throws GenieException {
        this.service.updateApplication(
                UUID.randomUUID().toString(), new Application());
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieBadRequestException.class)
    public void testUpdateApplicationIdsDontMatch() throws GenieException {
        final Application updateApp = new Application();
        updateApp.setId(UUID.randomUUID().toString());
        this.service.updateApplication(APP_1_ID, updateApp);
    }

    /**
     * Test delete all.
     *
     * @throws GenieException
     */
    @Test
    public void testDeleteAll() throws GenieException {
        Assert.assertEquals(3,
                this.service.getApplications(null, null, null, null, 0, 10, true, null).size());
        Assert.assertEquals(3, this.service.deleteAllApplications().size());
        Assert.assertTrue(
                this.service.getApplications(null, null, null, null, 0, 10, true, null)
                .isEmpty());
    }

    /**
     * Test delete.
     *
     * @throws GenieException
     */
    @Test
    public void testDelete() throws GenieException {
        Assert.assertEquals(APP_1_ID,
                this.commandService
                .getApplicationForCommand(COMMAND_1_ID)
                .getId());
        Assert.assertEquals(APP_1_ID,
                this.service.deleteApplication(APP_1_ID).getId());
        Assert.assertNull(this.commandService.getCommand(COMMAND_1_ID)
                .getApplication());

        //Test a case where the app has no commands to
        //make sure that also works.
        Assert.assertEquals(APP_3_ID,
                this.service.deleteApplication(APP_3_ID).getId());
    }

    /**
     * Test delete.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testDeleteNoId() throws GenieException {
        this.service.deleteApplication(null);
    }

    /**
     * Test delete.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testDeleteNoAppToDelete() throws GenieException {
        this.service.deleteApplication(UUID.randomUUID().toString());
    }

    /**
     * Test add configurations to application.
     *
     * @throws GenieException
     */
    @Test
    public void testAddConfigsToApplication() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = new HashSet<>();
        newConfigs.add(newConfig1);
        newConfigs.add(newConfig2);
        newConfigs.add(newConfig3);

        Assert.assertEquals(2,
                this.service.getConfigsForApplication(APP_1_ID).size());
        final Set<String> finalConfigs
                = this.service.addConfigsToApplication(APP_1_ID, newConfigs);
        Assert.assertEquals(5, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test add configurations to application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddConfigsToApplicationNoId() throws GenieException {
        this.service.addConfigsToApplication(null, new HashSet<String>());
    }

    /**
     * Test add configurations to application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddConfigsToApplicationNoConfigs() throws GenieException {
        this.service.addConfigsToApplication(APP_1_ID, null);
    }

    /**
     * Test add configurations to application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddConfigsToApplicationNoApp() throws GenieException {
        final Set<String> configs = new HashSet<>();
        configs.add(UUID.randomUUID().toString());
        this.service.addConfigsToApplication(UUID.randomUUID().toString(), configs);
    }

    /**
     * Test update configurations for application.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateConfigsForApplication() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = new HashSet<>();
        newConfigs.add(newConfig1);
        newConfigs.add(newConfig2);
        newConfigs.add(newConfig3);

        Assert.assertEquals(2,
                this.service.getConfigsForApplication(APP_1_ID).size());
        final Set<String> finalConfigs
                = this.service.updateConfigsForApplication(APP_1_ID, newConfigs);
        Assert.assertEquals(3, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test update configurations for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateConfigsForApplicationNoId() throws GenieException {
        this.service.updateConfigsForApplication(null, new HashSet<String>());
    }

    /**
     * Test update configurations for application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateConfigsForApplicationNoApp() throws GenieException {
        this.service.updateConfigsForApplication(UUID.randomUUID().toString(),
                new HashSet<String>());
    }

    /**
     * Test get configurations for application.
     *
     * @throws GenieException
     */
    @Test
    public void testGetConfigsForApplication() throws GenieException {
        Assert.assertEquals(2,
                this.service.getConfigsForApplication(APP_1_ID).size());
    }

    /**
     * Test get configurations to application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetConfigsForApplicationNoId() throws GenieException {
        this.service.getConfigsForApplication(null);
    }

    /**
     * Test get configurations to application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetConfigsForApplicationNoApp() throws GenieException {
        this.service.getConfigsForApplication(UUID.randomUUID().toString());
    }

    /**
     * Test remove all configurations for application.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveAllConfigsForApplication() throws GenieException {
        Assert.assertEquals(2,
                this.service.getConfigsForApplication(APP_1_ID).size());
        Assert.assertEquals(0,
                this.service.removeAllConfigsForApplication(APP_1_ID).size());
    }

    /**
     * Test remove all configurations for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllConfigsForApplicationNoId() throws GenieException {
        this.service.removeAllConfigsForApplication(null);
    }

    /**
     * Test remove all configurations for application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllConfigsForApplicationNoApp() throws GenieException {
        this.service.removeAllConfigsForApplication(UUID.randomUUID().toString());
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveConfigForApplication() throws GenieException {
        final Set<String> configs
                = this.service.getConfigsForApplication(APP_1_ID);
        Assert.assertEquals(2, configs.size());
        Assert.assertEquals(1,
                this.service.removeConfigForApplication(
                        APP_1_ID,
                        configs.iterator().next()).size());
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveConfigForApplicationNullConfig() throws GenieException {
        this.service.removeConfigForApplication(APP_1_ID, null);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveConfigForApplicationNoId() throws GenieException {
        this.service.removeConfigForApplication(null, "something");
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveConfigForApplicationNoApp() throws GenieException {
        this.service.removeConfigForApplication(
                UUID.randomUUID().toString(),
                "something");
    }

    /**
     * Test add jars to application.
     *
     * @throws GenieException
     */
    @Test
    public void testAddJarsToApplication() throws GenieException {
        final String newJar1 = UUID.randomUUID().toString();
        final String newJar2 = UUID.randomUUID().toString();
        final String newJar3 = UUID.randomUUID().toString();

        final Set<String> newJars = new HashSet<>();
        newJars.add(newJar1);
        newJars.add(newJar2);
        newJars.add(newJar3);

        Assert.assertEquals(2,
                this.service.getJarsForApplication(APP_1_ID).size());
        final Set<String> finalJars
                = this.service.addJarsForApplication(APP_1_ID, newJars);
        Assert.assertEquals(5, finalJars.size());
        Assert.assertTrue(finalJars.contains(newJar1));
        Assert.assertTrue(finalJars.contains(newJar2));
        Assert.assertTrue(finalJars.contains(newJar3));
    }

    /**
     * Test add jars to application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddJarsToApplicationNoId() throws GenieException {
        this.service.addJarsForApplication(null, new HashSet<String>());
    }

    /**
     * Test add jars to application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddJarsToApplicationNoJars() throws GenieException {
        this.service.addJarsForApplication(APP_1_ID, null);
    }

    /**
     * Test add jars to application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddJarsForApplicationNoApp() throws GenieException {
        final Set<String> jars = new HashSet<>();
        jars.add(UUID.randomUUID().toString());
        this.service.addJarsForApplication(UUID.randomUUID().toString(), jars);
    }

    /**
     * Test update jars for application.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateJarsForApplication() throws GenieException {
        final String newJar1 = UUID.randomUUID().toString();
        final String newJar2 = UUID.randomUUID().toString();
        final String newJar3 = UUID.randomUUID().toString();

        final Set<String> newJars = new HashSet<>();
        newJars.add(newJar1);
        newJars.add(newJar2);
        newJars.add(newJar3);

        Assert.assertEquals(2,
                this.service.getJarsForApplication(APP_1_ID).size());
        final Set<String> finalJars
                = this.service.updateJarsForApplication(APP_1_ID, newJars);
        Assert.assertEquals(3, finalJars.size());
        Assert.assertTrue(finalJars.contains(newJar1));
        Assert.assertTrue(finalJars.contains(newJar2));
        Assert.assertTrue(finalJars.contains(newJar3));
    }

    /**
     * Test update jars for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateJarsForApplicationNoId() throws GenieException {
        this.service.updateJarsForApplication(null, new HashSet<String>());
    }

    /**
     * Test update jars for application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateJarsForApplicationNoApp() throws GenieException {
        this.service.updateJarsForApplication(UUID.randomUUID().toString(),
                new HashSet<String>());
    }

    /**
     * Test get jars for application.
     *
     * @throws GenieException
     */
    @Test
    public void testGetJarsForApplication() throws GenieException {
        Assert.assertEquals(2,
                this.service.getJarsForApplication(APP_1_ID).size());
    }

    /**
     * Test get jars to application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetJarsForApplicationNoId() throws GenieException {
        this.service.getJarsForApplication(null);
    }

    /**
     * Test get jars to application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetJarsForApplicationNoApp() throws GenieException {
        this.service.getJarsForApplication(UUID.randomUUID().toString());
    }

    /**
     * Test remove all jars for application.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveAllJarsForApplication() throws GenieException {
        Assert.assertEquals(2,
                this.service.getJarsForApplication(APP_1_ID).size());
        Assert.assertEquals(0,
                this.service.removeAllJarsForApplication(APP_1_ID).size());
    }

    /**
     * Test remove all jars for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllJarsForApplicationNoId() throws GenieException {
        this.service.removeAllJarsForApplication(null);
    }

    /**
     * Test remove all jars for application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllJarsForApplicationNoApp() throws GenieException {
        this.service.removeAllJarsForApplication(UUID.randomUUID().toString());
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveJarForApplication() throws GenieException {
        final Set<String> jars
                = this.service.getJarsForApplication(APP_1_ID);
        Assert.assertEquals(2, jars.size());
        Assert.assertEquals(1,
                this.service.removeJarForApplication(
                        APP_1_ID,
                        jars.iterator().next()).size());
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveJarForApplicationNullJar() throws GenieException {
        this.service.removeJarForApplication(APP_1_ID, null);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveJarForApplicationNoId() throws GenieException {
        this.service.removeJarForApplication(null, "something");
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveJarForApplicationNoApp() throws GenieException {
        this.service.removeJarForApplication(
                UUID.randomUUID().toString(),
                "something");
    }

    /**
     * Test add tags to application.
     *
     * @throws GenieException
     */
    @Test
    public void testAddTagsToApplication() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = new HashSet<>();
        newTags.add(newTag1);
        newTags.add(newTag2);
        newTags.add(newTag3);

        Assert.assertEquals(3,
                this.service.getTagsForApplication(APP_1_ID).size());
        final Set<String> finalTags
                = this.service.addTagsForApplication(APP_1_ID, newTags);
        Assert.assertEquals(6, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test add tags to application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddTagsToApplicationNoId() throws GenieException {
        this.service.addTagsForApplication(null, new HashSet<String>());
    }

    /**
     * Test add tags to application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddTagsToApplicationNoTags() throws GenieException {
        this.service.addTagsForApplication(APP_1_ID, null);
    }

    /**
     * Test add tags to application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddTagsForApplicationNoApp() throws GenieException {
        final Set<String> tags = new HashSet<>();
        tags.add(UUID.randomUUID().toString());
        this.service.addTagsForApplication(UUID.randomUUID().toString(), tags);
    }

    /**
     * Test update tags for application.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateTagsForApplication() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = new HashSet<>();
        newTags.add(newTag1);
        newTags.add(newTag2);
        newTags.add(newTag3);

        Assert.assertEquals(3,
                this.service.getTagsForApplication(APP_1_ID).size());
        final Set<String> finalTags
                = this.service.updateTagsForApplication(APP_1_ID, newTags);
        Assert.assertEquals(5, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test update tags for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateTagsForApplicationNoId() throws GenieException {
        this.service.updateTagsForApplication(null, new HashSet<String>());
    }

    /**
     * Test update tags for application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateTagsForApplicationNoApp() throws GenieException {
        this.service.updateTagsForApplication(UUID.randomUUID().toString(),
                new HashSet<String>());
    }

    /**
     * Test get tags for application.
     *
     * @throws GenieException
     */
    @Test
    public void testGetTagsForApplication() throws GenieException {
        Assert.assertEquals(3,
                this.service.getTagsForApplication(APP_1_ID).size());
    }

    /**
     * Test get tags to application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetTagsForApplicationNoId() throws GenieException {
        this.service.getTagsForApplication(null);
    }

    /**
     * Test get tags to application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetTagsForApplicationNoApp() throws GenieException {
        this.service.getTagsForApplication(UUID.randomUUID().toString());
    }

    /**
     * Test remove all tags for application.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveAllTagsForApplication() throws GenieException {
        Assert.assertEquals(3,
                this.service.getTagsForApplication(APP_1_ID).size());
        final Set<String> finalTags
                = this.service.removeAllTagsForApplication(APP_1_ID);
        Assert.assertEquals(2,
                finalTags.size());
    }

    /**
     * Test remove all tags for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllTagsForApplicationNoId() throws GenieException {
        this.service.removeAllTagsForApplication(null);
    }

    /**
     * Test remove all tags for application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllTagsForApplicationNoApp() throws GenieException {
        this.service.removeAllTagsForApplication(UUID.randomUUID().toString());
    }

    /**
     * Test remove tag for application.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveTagForApplication() throws GenieException {
        final Set<String> tags
                = this.service.getTagsForApplication(APP_1_ID);
        Assert.assertEquals(3, tags.size());
        Assert.assertEquals(2,
                this.service.removeTagForApplication(
                        APP_1_ID,
                        "prod").size()
        );
    }

    /**
     * Test remove tag for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveTagForApplicationNullTag() throws GenieException {
        this.service.removeTagForApplication(APP_1_ID, null).size();
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveTagForApplicationNoId() throws GenieException {
        this.service.removeTagForApplication(null, "something");
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveTagForApplicationNoApp() throws GenieException {
        this.service.removeTagForApplication(
                UUID.randomUUID().toString(),
                "something"
        );
    }

    /**
     * Test the Get commands for application method.
     *
     * @throws GenieException
     */
    @Test
    public void testGetCommandsForApplication() throws GenieException {
        final List<Command> commands
                = this.service.getCommandsForApplication(APP_1_ID, null);
        Assert.assertEquals(1, commands.size());
        Assert.assertEquals(COMMAND_1_ID, commands.iterator().next().getId());
    }

    /**
     * Test the Get commands for application method.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetCommandsForApplicationNoId() throws GenieException {
        this.service.getCommandsForApplication("", null);
    }

    /**
     * Test the Get commands for application method.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetCommandsForApplicationNoApp() throws GenieException {
        this.service.getCommandsForApplication(UUID.randomUUID().toString(), null);
    }
}
