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

import com.github.fge.jsonpatch.JsonPatch;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.ApplicationMetadata;
import com.netflix.genie.common.internal.dto.v4.ApplicationRequest;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.validation.ConstraintViolationException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the JpaApplicationPersistenceServiceImpl.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(IntegrationTest.class)
@DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTests/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaApplicationPersistenceServiceImplIntegrationTests extends DBIntegrationTestBase {

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

    private static final Pageable PAGEABLE = PageRequest.of(0, 10, Sort.Direction.DESC, "updated");

    @Autowired
    private ApplicationPersistenceService appService;

    @Autowired
    private CommandPersistenceService commandPersistenceService;

    /**
     * Test the get application method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetApplication() throws GenieException {
        final Application app = this.appService.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_ID, app.getId());
        Assert.assertEquals(APP_1_NAME, app.getMetadata().getName());
        Assert.assertEquals(APP_1_USER, app.getMetadata().getUser());
        Assert.assertEquals(APP_1_VERSION, app.getMetadata().getVersion());
        Assert.assertEquals(APP_1_STATUS, app.getMetadata().getStatus());
        Assert.assertFalse(app.getMetadata().getType().isPresent());
        Assert.assertEquals(1, app.getMetadata().getTags().size());
        Assert.assertEquals(2, app.getResources().getConfigs().size());
        Assert.assertEquals(2, app.getResources().getDependencies().size());

        final Application app2 = this.appService.getApplication(APP_2_ID);
        Assert.assertEquals(APP_2_ID, app2.getId());
        Assert.assertEquals(APP_2_NAME, app2.getMetadata().getName());
        Assert.assertEquals(APP_2_USER, app2.getMetadata().getUser());
        Assert.assertEquals(APP_2_VERSION, app2.getMetadata().getVersion());
        Assert.assertEquals(APP_2_STATUS, app2.getMetadata().getStatus());
        Assert.assertThat(app2.getMetadata().getType().orElseGet(RandomSuppliers.STRING), Matchers.is(APP_2_TYPE));
        Assert.assertEquals(2, app2.getMetadata().getTags().size());
        Assert.assertEquals(2, app2.getResources().getConfigs().size());
        Assert.assertEquals(1, app2.getResources().getDependencies().size());

        final Application app3 = this.appService.getApplication(APP_3_ID);
        Assert.assertEquals(APP_3_ID, app3.getId());
        Assert.assertEquals(APP_3_NAME, app3.getMetadata().getName());
        Assert.assertEquals(APP_3_USER, app3.getMetadata().getUser());
        Assert.assertEquals(APP_3_VERSION, app3.getMetadata().getVersion());
        Assert.assertEquals(APP_3_STATUS, app3.getMetadata().getStatus());
        Assert.assertThat(app3.getMetadata().getType().orElseGet(RandomSuppliers.STRING), Matchers.is(APP_3_TYPE));
        Assert.assertEquals(1, app3.getMetadata().getTags().size());
        Assert.assertEquals(1, app3.getResources().getConfigs().size());
        Assert.assertEquals(2, app3.getResources().getDependencies().size());
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
        Assert.assertThat(apps.getContent().get(0).getId(), Matchers.is(APP_2_ID));
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByUser() {
        final Page<Application> apps = this.appService.getApplications(null, APP_1_USER, null, null, null, PAGEABLE);
        Assert.assertEquals(2, apps.getNumberOfElements());
        Assert.assertEquals(APP_3_ID, apps.getContent().get(0).getId());
        Assert.assertEquals(APP_1_ID, apps.getContent().get(1).getId());
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByStatuses() {
        final Set<ApplicationStatus> statuses = Sets.newHashSet(ApplicationStatus.ACTIVE, ApplicationStatus.INACTIVE);
        final Page<Application> apps = this.appService.getApplications(null, null, statuses, null, null, PAGEABLE);
        Assert.assertEquals(2, apps.getNumberOfElements());
        Assert.assertEquals(APP_2_ID, apps.getContent().get(0).getId());
        Assert.assertEquals(APP_1_ID, apps.getContent().get(1).getId());
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByTags() {
        final Set<String> tags = Sets.newHashSet("prod");
        Page<Application> apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assert.assertEquals(3, apps.getNumberOfElements());
        Assert.assertEquals(APP_3_ID, apps.getContent().get(0).getId());
        Assert.assertEquals(APP_2_ID, apps.getContent().get(1).getId());
        Assert.assertEquals(APP_1_ID, apps.getContent().get(2).getId());

        tags.add("yarn");
        apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assert.assertEquals(1, apps.getNumberOfElements());
        Assert.assertEquals(APP_2_ID, apps.getContent().get(0).getId());

        tags.add("somethingThatWouldNeverReallyExist");
        apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assert.assertThat(apps.getNumberOfElements(), Matchers.is(0));

        tags.clear();
        apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assert.assertEquals(3, apps.getNumberOfElements());
        Assert.assertEquals(APP_3_ID, apps.getContent().get(0).getId());
        Assert.assertEquals(APP_2_ID, apps.getContent().get(1).getId());
        Assert.assertEquals(APP_1_ID, apps.getContent().get(2).getId());
    }

    /**
     * Test the get applications method when a tag doesn't exist in the database.
     */
    @Test
    public void testGetApplicationsByTagsWhenOneDoesntExist() {
        final Set<String> tags = Sets.newHashSet("prod", UUID.randomUUID().toString());
        final Page<Application> apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assert.assertEquals(0, apps.getNumberOfElements());
    }

    /**
     * Test the get applications method.
     */
    @Test
    public void testGetApplicationsByType() {
        final Page<Application> apps = this.appService.getApplications(null, null, null, null, APP_2_TYPE, PAGEABLE);
        Assert.assertEquals(1, apps.getNumberOfElements());
        Assert.assertEquals(APP_2_ID, apps.getContent().get(0).getId());
    }

    /**
     * Test the get applications method with descending sort.
     */
    @Test
    public void testGetApplicationsDescending() {
        //Default to order by Updated
        final Page<Application> applications = this.appService.getApplications(null, null, null, null, null, PAGEABLE);
        Assert.assertEquals(3, applications.getNumberOfElements());
        Assert.assertEquals(APP_3_ID, applications.getContent().get(0).getId());
        Assert.assertEquals(APP_2_ID, applications.getContent().get(1).getId());
        Assert.assertEquals(APP_1_ID, applications.getContent().get(2).getId());
    }

    /**
     * Test the get applications method with ascending sort.
     */
    @Test
    public void testGetApplicationsAscending() {
        //Default to order by Updated
        final Pageable ascendingPage = PageRequest.of(0, 10, Sort.Direction.ASC, "updated");
        final Page<Application> applications
            = this.appService.getApplications(null, null, null, null, null, ascendingPage);
        Assert.assertEquals(3, applications.getNumberOfElements());
        Assert.assertEquals(APP_1_ID, applications.getContent().get(0).getId());
        Assert.assertEquals(APP_2_ID, applications.getContent().get(1).getId());
        Assert.assertEquals(APP_3_ID, applications.getContent().get(2).getId());
    }

    /**
     * Test the get applications method default order by.
     */
    @Test
    public void testGetApplicationsOrderBysDefault() {
        //Default to order by Updated
        final Page<Application> applications = this.appService.getApplications(null, null, null, null, null, PAGEABLE);
        Assert.assertEquals(3, applications.getNumberOfElements());
        Assert.assertEquals(APP_3_ID, applications.getContent().get(0).getId());
        Assert.assertEquals(APP_2_ID, applications.getContent().get(1).getId());
        Assert.assertEquals(APP_1_ID, applications.getContent().get(2).getId());
    }

    /**
     * Test the get applications method order by name.
     */
    @Test
    public void testGetApplicationsOrderBysName() {
        final Pageable orderByNamePage = PageRequest.of(0, 10, Sort.Direction.DESC, "name");
        final Page<Application> applications
            = this.appService.getApplications(null, null, null, null, null, orderByNamePage);
        Assert.assertEquals(3, applications.getNumberOfElements());
        Assert.assertEquals(APP_1_ID, applications.getContent().get(0).getId());
        Assert.assertEquals(APP_3_ID, applications.getContent().get(1).getId());
        Assert.assertEquals(APP_2_ID, applications.getContent().get(2).getId());
    }

    /**
     * Test the get applications method order by an invalid field should return the order by default value (updated).
     */
    @Test(expected = RuntimeException.class)
    public void testGetApplicationsOrderBysInvalidField() {
        final Pageable orderByInvalidPage = PageRequest.of(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
        this.appService.getApplications(null, null, null, null, null, orderByInvalidPage);
    }

    /**
     * Test the create method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateApplication() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final ApplicationRequest app = new ApplicationRequest.Builder(
            new ApplicationMetadata.Builder(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
            )
                .build()
        )
            .withRequestedId(id)
            .build();
        final String createdId = this.appService.createApplication(app);
        Assert.assertThat(createdId, Matchers.is(id));
        final Application created = this.appService.getApplication(id);
        Assert.assertNotNull(created);
        Assert.assertEquals(id, created.getId());
        Assert.assertEquals(APP_1_NAME, created.getMetadata().getName());
        Assert.assertEquals(APP_1_USER, created.getMetadata().getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, created.getMetadata().getStatus());
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
        final ApplicationRequest app = new ApplicationRequest.Builder(
            new ApplicationMetadata.Builder(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
            )
                .build()
        )
            .build();
        final String id = this.appService.createApplication(app);
        final Application created = this.appService.getApplication(id);
        Assert.assertNotNull(created);
        Assert.assertEquals(APP_1_NAME, created.getMetadata().getName());
        Assert.assertEquals(APP_1_USER, created.getMetadata().getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, created.getMetadata().getStatus());
        this.appService.deleteApplication(created.getId());
        try {
            this.appService.getApplication(created.getId());
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertEquals(
                HttpURLConnection.HTTP_NOT_FOUND,
                ge.getErrorCode()
            );
        }
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateApplication() throws GenieException {
        final Application getApp = this.appService.getApplication(APP_1_ID);
        Assert.assertEquals(APP_1_USER, getApp.getMetadata().getUser());
        Assert.assertEquals(ApplicationStatus.INACTIVE, getApp.getMetadata().getStatus());
        Assert.assertEquals(1, getApp.getMetadata().getTags().size());
        final Instant updateTime = getApp.getUpdated();

        final Set<String> tags = Sets.newHashSet("prod", "tez", "yarn", "hadoop");
        tags.addAll(getApp.getMetadata().getTags());
        final Application updateApp = new Application(
            getApp.getId(),
            getApp.getCreated(),
            getApp.getUpdated(),
            getApp.getResources(),
            new ApplicationMetadata.Builder(
                getApp.getMetadata().getName(),
                APP_2_USER,
                getApp.getMetadata().getVersion(),
                ApplicationStatus.ACTIVE
            )
                .withDescription(getApp.getMetadata().getDescription().orElse(null))
                .withType(getApp.getMetadata().getType().orElse(null))
                .withTags(tags)
                .build()
        );

        this.appService.updateApplication(APP_1_ID, updateApp);

        final Application updated = this.appService.getApplication(APP_1_ID);
        Assert.assertNotEquals(updated.getUpdated(), Matchers.is(updateTime));
        Assert.assertEquals(APP_2_USER, updated.getMetadata().getUser());
        Assert.assertEquals(ApplicationStatus.ACTIVE, updated.getMetadata().getStatus());
        Assert.assertEquals(tags, updated.getMetadata().getTags());
    }

    /**
     * Test to make sure setting the created and updated outside the system control doesn't change record in database.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateCreateAndUpdate() throws GenieException {
        final Application init = this.appService.getApplication(APP_1_ID);
        final Instant created = init.getCreated();
        final Instant updated = init.getUpdated();

        this.appService.updateApplication(APP_1_ID, init);

        final Application updatedApp = this.appService.getApplication(APP_1_ID);
        Assert.assertEquals(created, updatedApp.getCreated());
        Assert.assertThat(updatedApp.getUpdated(), Matchers.not(updated));
        Assert.assertNotEquals(Instant.EPOCH, updatedApp.getUpdated());
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
        Assert.assertEquals(APP_1_USER, getApp.getMetadata().getUser());
        final Instant updateTime = getApp.getUpdated();

        final String patchString
            = "[{ \"op\": \"replace\", \"path\": \"/metadata/user\", \"value\": \"" + APP_2_USER + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));

        this.appService.patchApplication(APP_1_ID, patch);

        final Application updated = this.appService.getApplication(APP_1_ID);
        Assert.assertNotEquals(updated.getUpdated(), Matchers.is(updateTime));
        Assert.assertEquals(APP_2_USER, updated.getMetadata().getUser());
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
        this.commandPersistenceService.deleteCommand(COMMAND_1_ID);
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
     * Test add configurations to application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddConfigsToApplication() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assert.assertEquals(2, this.appService.getConfigsForApplication(APP_1_ID).size());
        this.appService.addConfigsToApplication(APP_1_ID, newConfigs);
        final Set<String> finalConfigs = this.appService.getConfigsForApplication(APP_1_ID);
        Assert.assertEquals(5, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
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

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assert.assertEquals(2, this.appService.getConfigsForApplication(APP_1_ID).size());
        this.appService.updateConfigsForApplication(APP_1_ID, newConfigs);
        final Set<String> finalConfigs = this.appService.getConfigsForApplication(APP_1_ID);
        Assert.assertEquals(3, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
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
     * Test add dependencies to application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddDependenciesToApplication() throws GenieException {
        final String newDependency1 = UUID.randomUUID().toString();
        final String newDependency2 = UUID.randomUUID().toString();
        final String newDependency3 = UUID.randomUUID().toString();

        final Set<String> newDependencies = Sets.newHashSet(newDependency1, newDependency2, newDependency3);

        Assert.assertEquals(2, this.appService.getDependenciesForApplication(APP_1_ID).size());
        this.appService.addDependenciesForApplication(APP_1_ID, newDependencies);
        final Set<String> finalDependencies = this.appService.getDependenciesForApplication(APP_1_ID);
        Assert.assertEquals(5, finalDependencies.size());
        Assert.assertTrue(finalDependencies.contains(newDependency1));
        Assert.assertTrue(finalDependencies.contains(newDependency2));
        Assert.assertTrue(finalDependencies.contains(newDependency3));
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

        final Set<String> newDependencies = Sets.newHashSet(newDependency1, newDependency2, newDependency3);

        Assert.assertEquals(2, this.appService.getDependenciesForApplication(APP_1_ID).size());
        this.appService.updateDependenciesForApplication(APP_1_ID, newDependencies);
        final Set<String> finalDependencies = this.appService.getDependenciesForApplication(APP_1_ID);
        Assert.assertEquals(3, finalDependencies.size());
        Assert.assertTrue(finalDependencies.contains(newDependency1));
        Assert.assertTrue(finalDependencies.contains(newDependency2));
        Assert.assertTrue(finalDependencies.contains(newDependency3));
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
     * Test add tags to application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddTagsToApplication() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assert.assertEquals(1, this.appService.getTagsForApplication(APP_1_ID).size());
        this.appService.addTagsForApplication(APP_1_ID, newTags);
        final Set<String> finalTags = this.appService.getTagsForApplication(APP_1_ID);
        Assert.assertEquals(4, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
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

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assert.assertEquals(1, this.appService.getTagsForApplication(APP_1_ID).size());
        this.appService.updateTagsForApplication(APP_1_ID, newTags);
        final Set<String> finalTags = this.appService.getTagsForApplication(APP_1_ID);
        Assert.assertEquals(3, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test get tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetTagsForApplication() throws GenieException {
        Assert.assertEquals(1, this.appService.getTagsForApplication(APP_1_ID).size());
    }

    /**
     * Test remove all tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllTagsForApplication() throws GenieException {
        Assert.assertEquals(1, this.appService.getTagsForApplication(APP_1_ID).size());
        this.appService.removeAllTagsForApplication(APP_1_ID);
        Assert.assertEquals(0, this.appService.getTagsForApplication(APP_1_ID).size());
    }

    /**
     * Test remove tag for application.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveTagForApplication() throws GenieException {
        final Set<String> tags = this.appService.getTagsForApplication(APP_1_ID);
        Assert.assertEquals(1, tags.size());
        this.appService.removeTagForApplication(APP_1_ID, "prod");
        Assert.assertFalse(this.appService.getTagsForApplication(APP_1_ID).contains("prod"));
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
        Assert.assertEquals(COMMAND_1_ID, commands.iterator().next().getId());
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
