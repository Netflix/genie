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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PropertyReferenceException;

import javax.validation.ConstraintViolationException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the ApplicationServiceJPAImpl.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(IntegrationTest.class)
@DatabaseSetup("JpaApplicationServiceImplIntegrationTests/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaApplicationServiceImplIntegrationTests extends DBUnitTestBase {

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
    private static final String APP_2_TYPE = "spark";
    private static final ApplicationStatus APP_2_STATUS = ApplicationStatus.ACTIVE;

    private static final String APP_3_ID = "app3";
    private static final String APP_3_NAME = "storm";
    private static final String APP_3_USER = "tgianos";
    private static final String APP_3_VERSION = "7.8.9";
    private static final String APP_3_TYPE = "storm";
    private static final ApplicationStatus APP_3_STATUS = ApplicationStatus.DEPRECATED;

    private static final Pageable PAGEABLE = new PageRequest(0, 10, Sort.Direction.DESC, "updated");

    @Autowired
    private ApplicationService appService;

    @Autowired
    private CommandService commandService;

    /**
     * Test the get application method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetApplication() throws GenieException {
        final Application app = this.appService.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_ID, app.getId().orElseGet(RandomSuppliers.STRING));
        Assert.assertEquals(APP_1_NAME, app.getName());
        Assert.assertEquals(APP_1_USER, app.getUser());
        Assert.assertEquals(APP_1_VERSION, app.getVersion());
        Assert.assertEquals(APP_1_STATUS, app.getStatus());
        Assert.assertFalse(app.getType().isPresent());
        Assert.assertEquals(3, app.getTags().size());
        Assert.assertEquals(2, app.getConfigs().size());
        Assert.assertEquals(2, app.getDependencies().size());

        final Application app2 = this.appService.getApplication(APP_2_ID);
        Assert.assertEquals(APP_2_ID, app2.getId().orElseGet(RandomSuppliers.STRING));
        Assert.assertEquals(APP_2_NAME, app2.getName());
        Assert.assertEquals(APP_2_USER, app2.getUser());
        Assert.assertEquals(APP_2_VERSION, app2.getVersion());
        Assert.assertEquals(APP_2_STATUS, app2.getStatus());
        Assert.assertThat(app2.getType().orElseGet(RandomSuppliers.STRING), Matchers.is(APP_2_TYPE));
        Assert.assertEquals(4, app2.getTags().size());
        Assert.assertEquals(2, app2.getConfigs().size());
        Assert.assertEquals(1, app2.getDependencies().size());

        final Application app3 = this.appService.getApplication(APP_3_ID);
        Assert.assertEquals(APP_3_ID, app3.getId().orElseGet(RandomSuppliers.STRING));
        Assert.assertEquals(APP_3_NAME, app3.getName());
        Assert.assertEquals(APP_3_USER, app3.getUser());
        Assert.assertEquals(APP_3_VERSION, app3.getVersion());
        Assert.assertEquals(APP_3_STATUS, app3.getStatus());
        Assert.assertThat(app3.getType().orElseGet(RandomSuppliers.STRING), Matchers.is(APP_3_TYPE));
        Assert.assertEquals(3, app3.getTags().size());
        Assert.assertEquals(1, app3.getConfigs().size());
        Assert.assertEquals(2, app3.getDependencies().size());
    }

    /**
     * Test the get application method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetApplicationNull() throws GenieException {
        this.appService.getApplication(null);
    }

    /**
     * Test the get application method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetApplicationEmpty() throws GenieException {
        this.appService.getApplication("");
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByName() {
        final Page<Application> apps = this.appService.getApplications(APP_2_NAME, null, null, null, null, PAGEABLE);
        Assert.assertEquals(1, apps.getNumberOfElements());
        Assert.assertThat(
            apps.getContent().get(0).getId().orElseGet(RandomSuppliers.STRING),
            Matchers.is(APP_2_ID)
        );
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByUser() {
        final Page<Application> apps = this.appService.getApplications(null, APP_1_USER, null, null, null, PAGEABLE);
        Assert.assertEquals(2, apps.getNumberOfElements());
        Assert.assertEquals(APP_3_ID, apps.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(APP_1_ID, apps.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByStatuses() {
        final Set<ApplicationStatus> statuses = new HashSet<>();
        statuses.add(ApplicationStatus.ACTIVE);
        statuses.add(ApplicationStatus.INACTIVE);
        final Page<Application> apps = this.appService.getApplications(null, null, statuses, null, null, PAGEABLE);
        Assert.assertEquals(2, apps.getNumberOfElements());
        Assert.assertEquals(APP_2_ID, apps.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(APP_1_ID, apps.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByTags() {
        final Set<String> tags = new HashSet<>();
        tags.add("prod");
        Page<Application> apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assert.assertEquals(3, apps.getNumberOfElements());
        Assert.assertEquals(APP_3_ID, apps.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(APP_2_ID, apps.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(APP_1_ID, apps.getContent().get(2).getId().orElseThrow(IllegalArgumentException::new));

        tags.add("yarn");
        apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assert.assertEquals(1, apps.getNumberOfElements());
        Assert.assertEquals(APP_2_ID, apps.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new));

        tags.clear();
        tags.add("genie.name:spark");
        apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assert.assertEquals(1, apps.getNumberOfElements());
        Assert.assertEquals(APP_2_ID, apps.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new));

        tags.add("somethingThatWouldNeverReallyExist");
        apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assert.assertTrue(apps.getNumberOfElements() == 0);

        tags.clear();
        apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assert.assertEquals(3, apps.getNumberOfElements());
        Assert.assertEquals(APP_3_ID, apps.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(APP_2_ID, apps.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(APP_1_ID, apps.getContent().get(2).getId().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByType() {
        final Page<Application> apps = this.appService.getApplications(null, null, null, null, APP_2_TYPE, PAGEABLE);
        Assert.assertEquals(1, apps.getNumberOfElements());
        Assert.assertEquals(APP_2_ID, apps.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test the get applications method with descending sort.
     */
    @Test
    public void testGetClustersDescending() {
        //Default to order by Updated
        final Page<Application> applications = this.appService.getApplications(null, null, null, null, null, PAGEABLE);
        Assert.assertEquals(3, applications.getNumberOfElements());
        Assert.assertEquals(
            APP_3_ID, applications.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            APP_2_ID, applications.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            APP_1_ID, applications.getContent().get(2).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get applications method with ascending sort.
     */
    @Test
    public void testGetClustersAscending() {
        //Default to order by Updated
        final Pageable ascendingPage = new PageRequest(0, 10, Sort.Direction.ASC, "updated");
        final Page<Application> applications
            = this.appService.getApplications(null, null, null, null, null, ascendingPage);
        Assert.assertEquals(3, applications.getNumberOfElements());
        Assert.assertEquals(
            APP_1_ID, applications.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            APP_2_ID, applications.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            APP_3_ID, applications.getContent().get(2).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get applications method default order by.
     */
    @Test
    public void testGetClustersOrderBysDefault() {
        //Default to order by Updated
        final Page<Application> applications = this.appService.getApplications(null, null, null, null, null, PAGEABLE);
        Assert.assertEquals(3, applications.getNumberOfElements());
        Assert.assertEquals(
            APP_3_ID, applications.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            APP_2_ID, applications.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            APP_1_ID, applications.getContent().get(2).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get applications method order by name.
     */
    @Test
    public void testGetClustersOrderBysName() {
        final Pageable orderByNamePage = new PageRequest(0, 10, Sort.Direction.DESC, "name");
        final Page<Application> applications
            = this.appService.getApplications(null, null, null, null, null, orderByNamePage);
        Assert.assertEquals(3, applications.getNumberOfElements());
        Assert.assertEquals(
            APP_1_ID, applications.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            APP_3_ID, applications.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            APP_2_ID, applications.getContent().get(2).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get applications method order by an invalid field should return the order by default value (updated).
     */
    @Test(expected = PropertyReferenceException.class)
    public void testGetClustersOrderBysInvalidField() {
        final Pageable orderByInvalidPage = new PageRequest(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
        this.appService.getApplications(null, null, null, null, null, orderByInvalidPage);
    }

    /**
     * Test the get applications method order by a collection field should return the order by default value (updated).
     */
    @Test
    @Ignore
    public void testGetClustersOrderBysCollectionField() {
        final Pageable orderByTagsPage = new PageRequest(0, 10, Sort.Direction.DESC, "tags");
        final Page<Application> applications
            = this.appService.getApplications(null, null, null, null, null, orderByTagsPage);
        Assert.assertThat(applications.getContent().toString(), Matchers.is("blahs"));
        Assert.assertEquals(3, applications.getNumberOfElements());
        Assert.assertEquals(
            APP_3_ID, applications.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            APP_2_ID, applications.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            APP_1_ID, applications.getContent().get(2).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the create method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateApplication() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final Application app = new Application
            .Builder(APP_1_NAME, APP_1_USER, APP_1_VERSION, ApplicationStatus.ACTIVE)
            .withId(id)
            .build();
        final String createdId = this.appService.createApplication(app);
        Assert.assertThat(createdId, Matchers.is(id));
        final Application created = this.appService.getApplication(id);
        Assert.assertNotNull(created);
        Assert.assertEquals(id, created.getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(APP_1_NAME, created.getName());
        Assert.assertEquals(APP_1_USER, created.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, created.getStatus());
        this.appService.deleteApplication(id);
        try {
            this.appService.getApplication(id);
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
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateApplicationNoId() throws GenieException {
        final Application app = new Application
            .Builder(APP_1_NAME, APP_1_USER, APP_1_VERSION, ApplicationStatus.ACTIVE)
            .build();
        final String id = this.appService.createApplication(app);
        final Application created = this.appService.getApplication(id);
        Assert.assertNotNull(created);
        Assert.assertEquals(APP_1_NAME, created.getName());
        Assert.assertEquals(APP_1_USER, created.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, created.getStatus());
        this.appService.deleteApplication(created.getId().orElseThrow(IllegalArgumentException::new));
        try {
            this.appService.getApplication(created.getId().orElseThrow(IllegalArgumentException::new));
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
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testCreateApplicationNull() throws GenieException {
        this.appService.createApplication(null);
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateApplication() throws GenieException {
        final Application getApp = this.appService.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_USER, getApp.getUser());
        Assert.assertEquals(ApplicationStatus.INACTIVE, getApp.getStatus());
        Assert.assertEquals(3, getApp.getTags().size());
        final Date updateTime = getApp.getUpdated().orElseThrow(IllegalArgumentException::new);

        final Set<String> tags = new HashSet<>(getApp.getTags());
        tags.add("prod");
        tags.add("tez");
        tags.add("yarn");
        tags.add("hadoop");
        final Application.Builder updateApp = new Application
            .Builder(getApp.getName(), APP_2_USER, getApp.getVersion(), ApplicationStatus.ACTIVE)
            .withId(getApp.getId().orElseThrow(IllegalArgumentException::new))
            .withCreated(getApp.getCreated().orElseThrow(IllegalArgumentException::new))
            .withUpdated(getApp.getUpdated().orElseThrow(IllegalArgumentException::new))
            .withTags(tags)
            .withConfigs(getApp.getConfigs())
            .withDependencies(getApp.getDependencies());

        getApp.getDescription().ifPresent(updateApp::withDescription);
        getApp.getSetupFile().ifPresent(updateApp::withSetupFile);
        this.appService.updateApplication(APP_1_ID, updateApp.build());

        final Application updated = this.appService.getApplication(APP_1_ID);
        Assert.assertNotEquals(updated.getUpdated(), Matchers.is(updateTime));
        Assert.assertEquals(APP_2_USER, updated.getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, updated.getStatus());
        Assert.assertEquals(6, updated.getTags().size());
    }

    /**
     * Test to make sure setting the created and updated outside the system control doesn't change record in database.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateCreateAndUpdate() throws GenieException {
        final Application init = this.appService.getApplication(APP_1_ID);
        final Date created = init.getCreated().orElseThrow(IllegalArgumentException::new);
        final Date updated = init.getUpdated().orElseThrow(IllegalArgumentException::new);

        final Application.Builder updateApp = new Application.Builder(
            init.getName(), init.getUser(), init.getVersion(), init.getStatus()
        )
            .withId(init.getId().orElseThrow(IllegalArgumentException::new))
            .withCreated(new Date())
            .withUpdated(new Date(0))
            .withTags(init.getTags())
            .withConfigs(init.getConfigs())
            .withDependencies(init.getDependencies());

        init.getDescription().ifPresent(updateApp::withDescription);
        init.getSetupFile().ifPresent(updateApp::withSetupFile);

        this.appService.updateApplication(APP_1_ID, updateApp.build());

        final Application updatedApp = this.appService.getApplication(APP_1_ID);
        Assert.assertEquals(created, updatedApp.getCreated().orElseThrow(IllegalArgumentException::new));
        Assert.assertThat(updatedApp.getUpdated(), Matchers.not(updated));
        Assert.assertNotEquals(new Date(0), updatedApp.getUpdated());
    }

    /**
     * Test to patch an application.
     *
     * @throws GenieException For any problem
     * @throws IOException    For Json serialization problem
     */
    @Test
    public void testPatchApplication() throws GenieException, IOException {
        final Application getApp = this.appService.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_USER, getApp.getUser());
        final Date updateTime = getApp.getUpdated().orElseThrow(IllegalArgumentException::new);

        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/user\", \"value\": \"" + APP_2_USER + "\" }]";
        final ObjectMapper mapper = new ObjectMapper();
        final JsonPatch patch = JsonPatch.fromJson(mapper.readTree(patchString));

        this.appService.patchApplication(APP_1_ID, patch);

        final Application updated = this.appService.getApplication(APP_1_ID);
        Assert.assertNotEquals(updated.getUpdated(), Matchers.is(updateTime));
        Assert.assertEquals(APP_2_USER, updated.getUser());
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateApplicationNullId() throws GenieException {
        this.appService.updateApplication(null, this.appService.getApplication(APP_1_ID));
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateApplicationNullUpdateApp() throws GenieException {
        this.appService.updateApplication(APP_1_ID, null);
    }

    /**
     * Test delete all.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDeleteAll() throws GenieException {
        Assert.assertEquals(
            3,
            this.appService.getApplications(null, null, null, null, null, PAGEABLE).getNumberOfElements()
        );
        // To solve referential integrity problem
        this.commandService.deleteCommand(COMMAND_1_ID);
        this.appService.deleteAllApplications();
        Assert.assertTrue(
            this.appService.getApplications(null, null, null, null, null, PAGEABLE).getNumberOfElements() == 0
        );
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDelete() throws GenieException {
        this.appService.deleteApplication(APP_3_ID);
        try {
            this.appService.getApplication(APP_3_ID);
            Assert.fail();
        } catch (final GenieNotFoundException gnfe) {
            Assert.assertTrue(true);
        }
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testDeleteNoId() throws GenieException {
        this.appService.deleteApplication(null);
    }

    /**
     * Test add configurations to application.
     *
     * @throws GenieException For any problem
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

        Assert.assertEquals(2, this.appService.getConfigsForApplication(APP_1_ID).size());
        this.appService.addConfigsToApplication(APP_1_ID, newConfigs);
        final Set<String> finalConfigs = this.appService.getConfigsForApplication(APP_1_ID);
        Assert.assertEquals(5, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test add configurations to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddConfigsToApplicationNoId() throws GenieException {
        this.appService.addConfigsToApplication(null, new HashSet<>());
    }

    /**
     * Test add configurations to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddConfigsToApplicationNoConfigs() throws GenieException {
        this.appService.addConfigsToApplication(APP_1_ID, null);
    }

    /**
     * Test update configurations for application.
     *
     * @throws GenieException For any problem
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

        Assert.assertEquals(2, this.appService.getConfigsForApplication(APP_1_ID).size());
        this.appService.updateConfigsForApplication(APP_1_ID, newConfigs);
        final Set<String> finalConfigs = this.appService.getConfigsForApplication(APP_1_ID);
        Assert.assertEquals(3, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test update configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateConfigsForApplicationNoId() throws GenieException {
        this.appService.updateConfigsForApplication(null, new HashSet<>());
    }

    /**
     * Test get configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetConfigsForApplication() throws GenieException {
        Assert.assertEquals(2, this.appService.getConfigsForApplication(APP_1_ID).size());
    }

    /**
     * Test get configurations to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetConfigsForApplicationNoId() throws GenieException {
        this.appService.getConfigsForApplication(null);
    }

    /**
     * Test remove all configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllConfigsForApplication() throws GenieException {
        Assert.assertEquals(2, this.appService.getConfigsForApplication(APP_1_ID).size());
        this.appService.removeAllConfigsForApplication(APP_1_ID);
        Assert.assertEquals(0, this.appService.getConfigsForApplication(APP_1_ID).size());
    }

    /**
     * Test remove all configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllConfigsForApplicationNoId() throws GenieException {
        this.appService.removeAllConfigsForApplication(null);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveConfigForApplication() throws GenieException {
        final Set<String> configs = this.appService.getConfigsForApplication(APP_1_ID);
        Assert.assertEquals(2, configs.size());
        final String removedConfig = configs.iterator().next();
        this.appService.removeConfigForApplication(APP_1_ID, removedConfig);
        Assert.assertFalse(this.appService.getConfigsForApplication(APP_1_ID).contains(removedConfig));
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveConfigForApplicationNullConfig() throws GenieException {
        this.appService.removeConfigForApplication(APP_1_ID, null);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveConfigForApplicationNoId() throws GenieException {
        this.appService.removeConfigForApplication(null, "something");
    }

    /**
     * Test add dependencies to application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddDependenciesToApplication() throws GenieException {
        final String newDependency1 = UUID.randomUUID().toString();
        final String newDependency2 = UUID.randomUUID().toString();
        final String newDependency3 = UUID.randomUUID().toString();

        final Set<String> newDependencies = new HashSet<>();
        newDependencies.add(newDependency1);
        newDependencies.add(newDependency2);
        newDependencies.add(newDependency3);

        Assert.assertEquals(2, this.appService.getDependenciesForApplication(APP_1_ID).size());
        this.appService.addDependenciesForApplication(APP_1_ID, newDependencies);
        final Set<String> finalDependencies = this.appService.getDependenciesForApplication(APP_1_ID);
        Assert.assertEquals(5, finalDependencies.size());
        Assert.assertTrue(finalDependencies.contains(newDependency1));
        Assert.assertTrue(finalDependencies.contains(newDependency2));
        Assert.assertTrue(finalDependencies.contains(newDependency3));
    }

    /**
     * Test add dependencies to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddDependenciesToApplicationNoId() throws GenieException {
        this.appService.addDependenciesForApplication(null, new HashSet<>());
    }

    /**
     * Test add dependencies to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddDependenciesToApplicationNoDependencies() throws GenieException {
        this.appService.addDependenciesForApplication(APP_1_ID, null);
    }

    /**
     * Test update dependencies for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateDependenciesForApplication() throws GenieException {
        final String newDependency1 = UUID.randomUUID().toString();
        final String newDependency2 = UUID.randomUUID().toString();
        final String newDependency3 = UUID.randomUUID().toString();

        final Set<String> newDependencies = new HashSet<>();
        newDependencies.add(newDependency1);
        newDependencies.add(newDependency2);
        newDependencies.add(newDependency3);

        Assert.assertEquals(2, this.appService.getDependenciesForApplication(APP_1_ID).size());
        this.appService.updateDependenciesForApplication(APP_1_ID, newDependencies);
        final Set<String> finalDependencies = this.appService.getDependenciesForApplication(APP_1_ID);
        Assert.assertEquals(3, finalDependencies.size());
        Assert.assertTrue(finalDependencies.contains(newDependency1));
        Assert.assertTrue(finalDependencies.contains(newDependency2));
        Assert.assertTrue(finalDependencies.contains(newDependency3));
    }

    /**
     * Test update dependencies for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateDependenciesForApplicationNoId() throws GenieException {
        this.appService.updateDependenciesForApplication(null, new HashSet<>());
    }

    /**
     * Test get dependencies for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetDependenciesForApplication() throws GenieException {
        Assert.assertEquals(2,
            this.appService.getDependenciesForApplication(APP_1_ID).size());
    }

    /**
     * Test get dependencies to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetDependenciesForApplicationNoId() throws GenieException {
        this.appService.getDependenciesForApplication(null);
    }

    /**
     * Test remove all dependencies for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllDependenciesForApplication() throws GenieException {
        Assert.assertEquals(2, this.appService.getDependenciesForApplication(APP_1_ID).size());
        this.appService.removeAllDependenciesForApplication(APP_1_ID);
        Assert.assertEquals(0, this.appService.getDependenciesForApplication(APP_1_ID).size());
    }

    /**
     * Test remove all dependencies for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllDependenciesForApplicationNoId() throws GenieException {
        this.appService.removeAllDependenciesForApplication(null);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveDependencyForApplication() throws GenieException {
        final Set<String> dependencies = this.appService.getDependenciesForApplication(APP_1_ID);
        Assert.assertEquals(2, dependencies.size());
        final String removedDependency = dependencies.iterator().next();
        this.appService.removeDependencyForApplication(APP_1_ID, removedDependency);
        Assert.assertFalse(this.appService.getDependenciesForApplication(APP_1_ID).contains(removedDependency));
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveDependencyForApplicationNullDependency() throws GenieException {
        this.appService.removeDependencyForApplication(APP_1_ID, null);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveDependencyForApplicationNoId() throws GenieException {
        this.appService.removeDependencyForApplication(null, "something");
    }

    /**
     * Test add tags to application.
     *
     * @throws GenieException For any problem
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

        Assert.assertEquals(3, this.appService.getTagsForApplication(APP_1_ID).size());
        this.appService.addTagsForApplication(APP_1_ID, newTags);
        final Set<String> finalTags = this.appService.getTagsForApplication(APP_1_ID);
        Assert.assertEquals(6, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test add tags to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddTagsToApplicationNoId() throws GenieException {
        this.appService.addTagsForApplication(null, new HashSet<>());
    }

    /**
     * Test add tags to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddTagsToApplicationNoTags() throws GenieException {
        this.appService.addTagsForApplication(APP_1_ID, null);
    }

    /**
     * Test update tags for application.
     *
     * @throws GenieException For any problem
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

        Assert.assertEquals(3, this.appService.getTagsForApplication(APP_1_ID).size());
        this.appService.updateTagsForApplication(APP_1_ID, newTags);
        final Set<String> finalTags = this.appService.getTagsForApplication(APP_1_ID);
        Assert.assertEquals(5, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test update tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateTagsForApplicationNoId() throws GenieException {
        this.appService.updateTagsForApplication(null, new HashSet<>());
    }

    /**
     * Test get tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetTagsForApplication() throws GenieException {
        Assert.assertEquals(3,
            this.appService.getTagsForApplication(APP_1_ID).size());
    }

    /**
     * Test get tags to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetTagsForApplicationNoId() throws GenieException {
        this.appService.getTagsForApplication(null);
    }

    /**
     * Test remove all tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllTagsForApplication() throws GenieException {
        Assert.assertEquals(3, this.appService.getTagsForApplication(APP_1_ID).size());
        this.appService.removeAllTagsForApplication(APP_1_ID);
        Assert.assertEquals(2, this.appService.getTagsForApplication(APP_1_ID).size());
    }

    /**
     * Test remove all tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllTagsForApplicationNoId() throws GenieException {
        this.appService.removeAllTagsForApplication(null);
    }

    /**
     * Test remove tag for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveTagForApplication() throws GenieException {
        final Set<String> tags = this.appService.getTagsForApplication(APP_1_ID);
        Assert.assertEquals(3, tags.size());
        this.appService.removeTagForApplication(APP_1_ID, "prod");
        Assert.assertFalse(this.appService.getTagsForApplication(APP_1_ID).contains("prod"));
    }

    /**
     * Test remove tag for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveTagForApplicationNullTag() throws GenieException {
        this.appService.removeTagForApplication(APP_1_ID, null);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveTagForApplicationNoId() throws GenieException {
        this.appService.removeTagForApplication(null, "something");
    }

    /**
     * Test the Get commands for application method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetCommandsForApplication() throws GenieException {
        final Set<Command> commands = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assert.assertEquals(1, commands.size());
        Assert.assertEquals(
            COMMAND_1_ID, commands.iterator().next().getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the Get commands for application method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetCommandsForApplicationNoId() throws GenieException {
        this.appService.getCommandsForApplication("", null);
    }
}
