/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import com.github.springtestdbunit.TransactionDbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseOperation;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.ApplicationStatus;
import com.netflix.genie.server.services.ApplicationConfigService;
import java.net.HttpURLConnection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;

import javax.inject.Inject;

/**
 * Tests for the ApplicationConfigServiceJPAImpl.
 *
 * @author tgianos
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:genie-application-test.xml")
@TestExecutionListeners({
    DependencyInjectionTestExecutionListener.class,
    DirtiesContextTestExecutionListener.class,
    TransactionalTestExecutionListener.class,
    TransactionDbUnitTestExecutionListener.class
})
public class TestApplicationConfigServiceJPAImpl {

    private static final String APP_1_ID = "app1";
    private static final String APP_1_NAME = "tez";
    private static final String APP_1_USER = "tgianos";
    private static final String APP_1_VERSION = "1.2.3";
    private static final ApplicationStatus APP_1_STATUS
            = ApplicationStatus.INACTIVE;

    private static final String APP_2_ID = "app2";
    private static final String APP_2_NAME = "spark";
    private static final String APP_2_USER = "amsharma";
    private static final String APP_2_VERSION = "4.5.6";
    private static final ApplicationStatus APP_2_STATUS
            = ApplicationStatus.ACTIVE;

    private static final String APP_3_ID = "app3";
    private static final String APP_3_NAME = "storm";
    private static final String APP_3_USER = "tgianos";
    private static final String APP_3_VERSION = "7.8.9";
    private static final ApplicationStatus APP_3_STATUS
            = ApplicationStatus.DEPRECATED;

    @Inject
    private ApplicationConfigService service;

    /**
     * Test the get application method.
     *
     * @throws GenieException
     */
    @Test
    @DatabaseSetup("application/init.xml")
    @DatabaseTearDown(
            value = "application/init.xml",
            type = DatabaseOperation.DELETE_ALL)
    public void testGetApplication() throws GenieException {
        final Application app = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_ID, app.getId());
        Assert.assertEquals(APP_1_NAME, app.getName());
        Assert.assertEquals(APP_1_USER, app.getUser());
        Assert.assertEquals(APP_1_VERSION, app.getVersion());
        Assert.assertEquals(APP_1_STATUS, app.getStatus());
        Assert.assertEquals(4, app.getTags().size());
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
    @Test(expected = GenieException.class)
    public void testGetApplicationNull() throws GenieException {
        this.service.getApplication(null);
    }

    /**
     * Test the get application method.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testGetApplicationNotExists() throws GenieException {
        this.service.getApplication(UUID.randomUUID().toString());
    }

    /**
     * Test the get applications method.
     */
    @Test
    @DatabaseSetup("application/init.xml")
    @DatabaseTearDown(
            value = "application/init.xml",
            type = DatabaseOperation.DELETE_ALL)
    public void testGetApplicationsByName() {
        final List<Application> apps = this.service.getApplications(
                APP_2_NAME, null, null, 0, 10);
        Assert.assertEquals(1, apps.size());
        Assert.assertEquals(APP_2_ID, apps.get(0).getId());
    }

    /**
     * Test the get applications method.
     */
    @Test
    @DatabaseSetup("application/init.xml")
    @DatabaseTearDown(
            value = "application/init.xml",
            type = DatabaseOperation.DELETE_ALL)
    public void testGetApplicationsByUserName() {
        final List<Application> apps = this.service.getApplications(
                null, APP_1_USER, null, -1, -5000);
        Assert.assertEquals(2, apps.size());
        Assert.assertEquals(APP_3_ID, apps.get(0).getId());
        Assert.assertEquals(APP_1_ID, apps.get(1).getId());
    }

    /**
     * Test the get applications method.
     */
    @Test
    @DatabaseSetup("application/init.xml")
    @DatabaseTearDown(
            value = "application/init.xml",
            type = DatabaseOperation.DELETE_ALL)
    public void testGetApplicationsByTags() {
        final Set<String> tags = new HashSet<String>();
        tags.add("prod");
        List<Application> apps = this.service.getApplications(
                null, null, tags, 0, 10);
        Assert.assertEquals(3, apps.size());
        Assert.assertEquals(APP_3_ID, apps.get(0).getId());
        Assert.assertEquals(APP_2_ID, apps.get(1).getId());
        Assert.assertEquals(APP_1_ID, apps.get(2).getId());

        tags.add("yarn");
        apps = this.service.getApplications(
                null, null, tags, 0, 10);
        Assert.assertEquals(2, apps.size());
        Assert.assertEquals(APP_2_ID, apps.get(0).getId());
        Assert.assertEquals(APP_1_ID, apps.get(1).getId());

        tags.clear();
        tags.add("spark");
        apps = this.service.getApplications(
                null, null, tags, 0, 10);
        Assert.assertEquals(1, apps.size());
        Assert.assertEquals(APP_2_ID, apps.get(0).getId());

        tags.add("somethingThatWouldNeverReallyExist");
        apps = this.service.getApplications(
                null, null, tags, 0, 10);
        Assert.assertTrue(apps.isEmpty());

        tags.clear();
        apps = this.service.getApplications(
                null, null, tags, 0, 10);
        Assert.assertEquals(3, apps.size());
        Assert.assertEquals(APP_3_ID, apps.get(0).getId());
        Assert.assertEquals(APP_2_ID, apps.get(1).getId());
        Assert.assertEquals(APP_1_ID, apps.get(2).getId());
    }

    /**
     * Test the create method.
     *
     * @throws GenieException
     */
    @Test
    public void testCreateApplication() throws GenieException {
        try {
            this.service.getApplication(APP_1_ID);
            Assert.fail("Should have thrown exception");
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    ge.getErrorCode()
            );
        }
        final Application app = new Application(
                APP_1_NAME,
                APP_1_USER,
                ApplicationStatus.ACTIVE,
                APP_1_VERSION
        );
        app.setId(APP_1_ID);
        final Application created = this.service.createApplication(app);
        Assert.assertNotNull(this.service.getApplication(APP_1_ID));
        Assert.assertEquals(APP_1_ID, created.getId());
        Assert.assertEquals(APP_1_NAME, created.getName());
        Assert.assertEquals(APP_1_USER, created.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, created.getStatus());
        this.service.deleteApplication(APP_1_ID);
        try {
            this.service.getApplication(APP_1_ID);
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
        Assert.assertTrue(
                this.service.getApplications(
                        null,
                        null,
                        null,
                        0,
                        Integer.MAX_VALUE
                ).isEmpty());
        final Application app = new Application(
                APP_1_NAME,
                APP_1_USER,
                ApplicationStatus.ACTIVE,
                APP_1_VERSION
        );
        final Application created = this.service.createApplication(app);
        Assert.assertNotNull(this.service.getApplication(created.getId()));
        Assert.assertEquals(APP_1_NAME, created.getName());
        Assert.assertEquals(APP_1_USER, created.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, created.getStatus());
        this.service.deleteApplication(created.getId());
        try {
            this.service.getApplication(created.getId());
            Assert.fail("Should have thrown exception");
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
    @Test(expected = GenieException.class)
    public void testCreateApplicationNull() throws GenieException {
        this.service.createApplication(null);
    }

    /**
     * Test to make sure an exception is thrown when application already exists.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    @DatabaseSetup("application/init.xml")
    @DatabaseTearDown(
            value = "application/init.xml",
            type = DatabaseOperation.DELETE_ALL)
    public void testCreateApplicationAlreadyExists() throws GenieException {
        final Application app = new Application(
                APP_1_NAME,
                APP_1_USER,
                ApplicationStatus.ACTIVE,
                APP_1_VERSION
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
    @DatabaseSetup("application/init.xml")
    @DatabaseTearDown(
            value = "application/init.xml",
            type = DatabaseOperation.DELETE_ALL)
    public void testUpdateApplicationNoId() throws GenieException {
        final Application init = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_USER, init.getUser());
        Assert.assertEquals(ApplicationStatus.INACTIVE, init.getStatus());
        Assert.assertEquals(4, init.getTags().size());

        final Application updateApp = new Application();
        updateApp.setStatus(ApplicationStatus.ACTIVE);
        updateApp.setUser(APP_2_USER);
        final Set<String> tags = new HashSet<String>();
        tags.add("prod");
        tags.add("tez");
        tags.add("yarn");
        tags.add("hadoop");
        updateApp.setTags(tags);
        this.service.updateApplication(APP_1_ID, updateApp);

        final Application updated = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_2_USER, updated.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, updated.getStatus());
        Assert.assertEquals(5, updated.getTags().size());
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test
    @DatabaseSetup("application/init.xml")
    @DatabaseTearDown(
            value = "application/init.xml",
            type = DatabaseOperation.DELETE_ALL)
    public void testUpdateApplicationWithId() throws GenieException {
        final Application init = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_USER, init.getUser());
        Assert.assertEquals(ApplicationStatus.INACTIVE, init.getStatus());
        Assert.assertEquals(4, init.getTags().size());

        final Application updateApp = new Application();
        updateApp.setId(APP_1_ID);
        updateApp.setStatus(ApplicationStatus.ACTIVE);
        updateApp.setUser(APP_2_USER);
        final Set<String> tags = new HashSet<String>();
        tags.add("prod");
        tags.add("tez");
        tags.add("yarn");
        tags.add("hadoop");
        updateApp.setTags(tags);
        this.service.updateApplication(APP_1_ID, updateApp);

        final Application updated = this.service.getApplication(APP_1_ID);
        Assert.assertEquals(APP_2_USER, updated.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, updated.getStatus());
        Assert.assertEquals(5, updated.getTags().size());
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testUpdateApplicationNullId() throws GenieException {
        this.service.updateApplication(null, new Application());
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testUpdateApplicationNullUpdateApp() throws GenieException {
        this.service.updateApplication(APP_1_ID, null);
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testUpdateApplicationNoAppExists() throws GenieException {
        this.service.updateApplication(
                UUID.randomUUID().toString(), new Application());
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    @DatabaseSetup("application/init.xml")
    @DatabaseTearDown(
            value = "application/init.xml",
            type = DatabaseOperation.DELETE_ALL)
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
    @DatabaseSetup("application/init.xml")
    @DatabaseTearDown(
            value = "application/init.xml",
            type = DatabaseOperation.DELETE_ALL)
    public void testDeleteAll() throws GenieException {
        Assert.assertEquals(3,
                this.service.getApplications(null, null, null, 0, 10).size());
        Assert.assertEquals(3, this.service.deleteAllApplications().size());
        Assert.assertTrue(
                this.service.getApplications(null, null, null, 0, 10)
                .isEmpty());
    }
}
