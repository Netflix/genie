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

import com.github.fge.jsonpatch.JsonPatch;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.ApplicationMetadata;
import com.netflix.genie.common.external.dtos.v4.ApplicationRequest;
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.web.data.services.CommandPersistenceService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.validation.ConstraintViolationException;
import java.io.IOException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for {@link JpaApplicationPersistenceServiceImpl}.
 *
 * @author tgianos
 * @since 2.0.0
 */
@DatabaseTearDown("cleanup.xml")
class JpaApplicationPersistenceServiceImplIntegrationTest extends DBIntegrationTestBase {

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
    private JpaApplicationPersistenceServiceImpl appService;

    @Autowired
    private CommandPersistenceService commandPersistenceService;

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplication() throws GenieException {
        final Application app = this.appService.getApplication(APP_1_ID);
        Assertions.assertThat(app.getId()).isEqualTo(APP_1_ID);
        final ApplicationMetadata appMetadata = app.getMetadata();
        Assertions.assertThat(appMetadata.getName()).isEqualTo(APP_1_NAME);
        Assertions.assertThat(appMetadata.getUser()).isEqualTo(APP_1_USER);
        Assertions.assertThat(appMetadata.getVersion()).isEqualTo(APP_1_VERSION);
        Assertions.assertThat(appMetadata.getStatus()).isEqualByComparingTo(APP_1_STATUS);
        Assertions.assertThat(appMetadata.getType()).isNotPresent();
        Assertions.assertThat(appMetadata.getTags()).hasSize(1);
        Assertions.assertThat(app.getResources().getConfigs()).hasSize(2);
        Assertions.assertThat(app.getResources().getDependencies()).hasSize(2);

        final Application app2 = this.appService.getApplication(APP_2_ID);
        Assertions.assertThat(app2.getId()).isEqualTo(APP_2_ID);
        final ApplicationMetadata app2Metadata = app2.getMetadata();
        Assertions.assertThat(app2Metadata.getName()).isEqualTo(APP_2_NAME);
        Assertions.assertThat(app2Metadata.getUser()).isEqualTo(APP_2_USER);
        Assertions.assertThat(app2Metadata.getVersion()).isEqualTo(APP_2_VERSION);
        Assertions.assertThat(app2Metadata.getStatus()).isEqualByComparingTo(APP_2_STATUS);
        Assertions.assertThat(app2Metadata.getType()).isPresent().contains(APP_2_TYPE);
        Assertions.assertThat(app2Metadata.getTags()).hasSize(2);
        Assertions.assertThat(app2.getResources().getConfigs()).hasSize(2);
        Assertions.assertThat(app2.getResources().getDependencies()).hasSize(1);

        final Application app3 = this.appService.getApplication(APP_3_ID);
        Assertions.assertThat(app3.getId()).isEqualTo(APP_3_ID);
        final ApplicationMetadata app3Metadata = app3.getMetadata();
        Assertions.assertThat(app3Metadata.getName()).isEqualTo(APP_3_NAME);
        Assertions.assertThat(app3Metadata.getUser()).isEqualTo(APP_3_USER);
        Assertions.assertThat(app3Metadata.getVersion()).isEqualTo(APP_3_VERSION);
        Assertions.assertThat(app3Metadata.getStatus()).isEqualByComparingTo(APP_3_STATUS);
        Assertions.assertThat(app3Metadata.getType()).isPresent().contains(APP_3_TYPE);
        Assertions.assertThat(app3Metadata.getTags()).hasSize(1);
        Assertions.assertThat(app3.getResources().getConfigs()).hasSize(1);
        Assertions.assertThat(app3.getResources().getDependencies()).hasSize(2);
    }

    @Test
    void testGetApplicationEmpty() {
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.appService.getApplication(""));
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplicationsByName() {
        final Page<Application> apps = this.appService.getApplications(APP_2_NAME, null, null, null, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_2_ID);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplicationsByUser() {
        final Page<Application> apps = this.appService.getApplications(null, APP_1_USER, null, null, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_1_ID);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplicationsByStatuses() {
        final Set<ApplicationStatus> statuses = Sets.newHashSet(ApplicationStatus.ACTIVE, ApplicationStatus.INACTIVE);
        final Page<Application> apps = this.appService.getApplications(null, null, statuses, null, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_1_ID);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplicationsByTags() {
        final Set<String> tags = Sets.newHashSet("prod");
        Page<Application> apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_1_ID);

        tags.add("yarn");
        apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_2_ID);

        tags.add("somethingThatWouldNeverReallyExist");
        apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(0);

        tags.clear();
        apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_1_ID);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplicationsByTagsWhenOneDoesntExist() {
        final Set<String> tags = Sets.newHashSet("prod", UUID.randomUUID().toString());
        final Page<Application> apps = this.appService.getApplications(null, null, null, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(0);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplicationsByType() {
        final Page<Application> apps = this.appService.getApplications(null, null, null, null, APP_2_TYPE, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_2_ID);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplicationsDescending() {
        //Default to order by Updated
        final Page<Application> apps = this.appService.getApplications(null, null, null, null, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_1_ID);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplicationsAscending() {
        //Default to order by Updated
        final Pageable ascendingPage = PageRequest.of(0, 10, Sort.Direction.ASC, "updated");
        final Page<Application> apps = this.appService.getApplications(null, null, null, null, null, ascendingPage);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_1_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_3_ID);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplicationsOrderBysDefault() {
        //Default to order by Updated
        final Page<Application> apps = this.appService.getApplications(null, null, null, null, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_1_ID);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetApplicationsOrderBysName() {
        final Pageable orderByNamePage = PageRequest.of(0, 10, Sort.Direction.DESC, "name");
        final Page<Application> apps = this.appService.getApplications(null, null, null, null, null, orderByNamePage);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_1_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_2_ID);
    }

    /**
     * Test the get applications method order by an invalid field should return the order by default value (updated).
     */
    @Test
    void testGetApplicationsOrderBysInvalidField() {
        final Pageable orderByInvalidPage = PageRequest.of(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
        Assertions
            .assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> this.appService.getApplications(null, null, null, null, null, orderByInvalidPage));
    }

    @Test
    void testCreateApplication() throws GenieException {
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
        Assertions.assertThat(createdId).isEqualTo(id);
        final Application created = this.appService.getApplication(id);
        Assertions.assertThat(created.getId()).isEqualTo(createdId);
        final ApplicationMetadata appMetadata = created.getMetadata();
        Assertions.assertThat(appMetadata.getName()).isEqualTo(APP_1_NAME);
        Assertions.assertThat(appMetadata.getUser()).isEqualTo(APP_1_USER);
        Assertions.assertThat(appMetadata.getStatus()).isEqualByComparingTo(ApplicationStatus.ACTIVE);
        this.appService.deleteApplication(id);
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.getApplication(id));
    }

    @Test
    void testCreateApplicationNoId() throws GenieException {
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
        final ApplicationMetadata appMetadata = created.getMetadata();
        Assertions.assertThat(appMetadata.getName()).isEqualTo(APP_1_NAME);
        Assertions.assertThat(appMetadata.getUser()).isEqualTo(APP_1_USER);
        Assertions.assertThat(appMetadata.getStatus()).isEqualByComparingTo(ApplicationStatus.ACTIVE);
        this.appService.deleteApplication(id);
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.getApplication(id));
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateApplication() throws GenieException {
        final Application getApp = this.appService.getApplication(APP_1_ID);
        Assertions.assertThat(getApp.getMetadata().getUser()).isEqualTo(APP_1_USER);
        Assertions.assertThat(getApp.getMetadata().getStatus()).isEqualByComparingTo(ApplicationStatus.INACTIVE);
        Assertions.assertThat(getApp.getMetadata().getTags().size()).isEqualTo(1);
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
        Assertions.assertThat(updated.getUpdated()).isNotEqualTo(updateTime);
        Assertions.assertThat(updated.getMetadata().getUser()).isEqualTo(APP_2_USER);
        Assertions.assertThat(updated.getMetadata().getStatus()).isEqualByComparingTo(ApplicationStatus.ACTIVE);
        Assertions.assertThat(updated.getMetadata().getTags()).isEqualTo(tags);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateCreateAndUpdate() throws GenieException {
        final Application init = this.appService.getApplication(APP_1_ID);
        final Instant created = init.getCreated();
        final Instant updated = init.getUpdated();

        this.appService.updateApplication(APP_1_ID, init);

        final Application updatedApp = this.appService.getApplication(APP_1_ID);
        Assertions.assertThat(updatedApp.getCreated()).isEqualTo(created);
        Assertions.assertThat(updatedApp.getUpdated()).isNotEqualTo(updated).isNotEqualTo(Instant.EPOCH);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testPatchApplication() throws GenieException, IOException {
        final Application getApp = this.appService.getApplication(APP_1_ID);
        Assertions.assertThat(getApp.getMetadata().getUser()).isEqualTo(APP_1_USER);
        final Instant updateTime = getApp.getUpdated();

        final String patchString
            = "[{ \"op\": \"replace\", \"path\": \"/metadata/user\", \"value\": \"" + APP_2_USER + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));

        this.appService.patchApplication(APP_1_ID, patch);

        final Application updated = this.appService.getApplication(APP_1_ID);
        Assertions.assertThat(updated.getUpdated()).isNotEqualTo(updateTime);
        Assertions.assertThat(updated.getMetadata().getUser()).isEqualTo(APP_2_USER);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testDeleteAll() throws GenieException {
        Assertions
            .assertThat(this.appService.getApplications(null, null, null, null, null, PAGEABLE).getNumberOfElements())
            .isEqualTo(3);
        // To solve referential integrity problem
        this.commandPersistenceService.deleteCommand(COMMAND_1_ID);
        this.appService.deleteAllApplications();
        Assertions
            .assertThat(this.appService.getApplications(null, null, null, null, null, PAGEABLE).getNumberOfElements())
            .isEqualTo(0);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testDelete() throws GenieException {
        this.appService.deleteApplication(APP_3_ID);
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.appService.getApplication(APP_3_ID));
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testAddConfigsToApplication() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assertions.assertThat(this.appService.getConfigsForApplication(APP_1_ID)).hasSize(2);
        this.appService.addConfigsToApplication(APP_1_ID, newConfigs);
        final Set<String> finalConfigs = this.appService.getConfigsForApplication(APP_1_ID);
        Assertions.assertThat(finalConfigs).hasSize(5).containsAll(newConfigs);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateConfigsForApplication() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assertions.assertThat(this.appService.getConfigsForApplication(APP_1_ID)).hasSize(2);
        this.appService.updateConfigsForApplication(APP_1_ID, newConfigs);
        final Set<String> finalConfigs = this.appService.getConfigsForApplication(APP_1_ID);
        Assertions.assertThat(finalConfigs).hasSize(3).containsAll(newConfigs);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetConfigsForApplication() throws GenieException {
        Assertions.assertThat(this.appService.getConfigsForApplication(APP_1_ID)).hasSize(2);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testRemoveAllConfigsForApplication() throws GenieException {
        Assertions.assertThat(this.appService.getConfigsForApplication(APP_1_ID)).hasSize(2);
        this.appService.removeAllConfigsForApplication(APP_1_ID);
        Assertions.assertThat(this.appService.getConfigsForApplication(APP_1_ID)).isEmpty();
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testRemoveConfigForApplication() throws GenieException {
        final Set<String> configs = this.appService.getConfigsForApplication(APP_1_ID);
        Assertions.assertThat(configs).hasSize(2);
        final String removedConfig = configs.iterator().next();
        this.appService.removeConfigForApplication(APP_1_ID, removedConfig);
        Assertions.assertThat(this.appService.getConfigsForApplication(APP_1_ID)).doesNotContain(removedConfig);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testAddDependenciesToApplication() throws GenieException {
        final String newDependency1 = UUID.randomUUID().toString();
        final String newDependency2 = UUID.randomUUID().toString();
        final String newDependency3 = UUID.randomUUID().toString();

        final Set<String> newDependencies = Sets.newHashSet(newDependency1, newDependency2, newDependency3);

        Assertions.assertThat(this.appService.getDependenciesForApplication(APP_1_ID)).hasSize(2);
        this.appService.addDependenciesForApplication(APP_1_ID, newDependencies);
        final Set<String> finalDependencies = this.appService.getDependenciesForApplication(APP_1_ID);
        Assertions.assertThat(finalDependencies).hasSize(5).containsAll(newDependencies);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateDependenciesForApplication() throws GenieException {
        final String newDependency1 = UUID.randomUUID().toString();
        final String newDependency2 = UUID.randomUUID().toString();
        final String newDependency3 = UUID.randomUUID().toString();

        final Set<String> newDependencies = Sets.newHashSet(newDependency1, newDependency2, newDependency3);

        Assertions.assertThat(this.appService.getDependenciesForApplication(APP_1_ID)).hasSize(2);
        this.appService.updateDependenciesForApplication(APP_1_ID, newDependencies);
        final Set<String> finalDependencies = this.appService.getDependenciesForApplication(APP_1_ID);
        Assertions.assertThat(finalDependencies).hasSize(3).containsAll(newDependencies);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetDependenciesForApplication() throws GenieException {
        Assertions.assertThat(this.appService.getDependenciesForApplication(APP_1_ID)).hasSize(2);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testRemoveAllDependenciesForApplication() throws GenieException {
        Assertions.assertThat(this.appService.getDependenciesForApplication(APP_1_ID)).hasSize(2);
        this.appService.removeAllDependenciesForApplication(APP_1_ID);
        Assertions.assertThat(this.appService.getDependenciesForApplication(APP_1_ID)).isEmpty();
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testRemoveDependencyForApplication() throws GenieException {
        final Set<String> dependencies = this.appService.getDependenciesForApplication(APP_1_ID);
        Assertions.assertThat(dependencies).hasSize(2);
        final String removedDependency = dependencies.iterator().next();
        this.appService.removeDependencyForApplication(APP_1_ID, removedDependency);
        Assertions
            .assertThat(this.appService.getDependenciesForApplication(APP_1_ID))
            .doesNotContain(removedDependency);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testAddTagsToApplication() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assertions.assertThat(this.appService.getTagsForApplication(APP_1_ID)).hasSize(1);
        this.appService.addTagsForApplication(APP_1_ID, newTags);
        final Set<String> finalTags = this.appService.getTagsForApplication(APP_1_ID);
        Assertions.assertThat(finalTags).hasSize(4).containsAll(newTags);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateTagsForApplication() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assertions.assertThat(this.appService.getTagsForApplication(APP_1_ID)).hasSize(1);
        this.appService.updateTagsForApplication(APP_1_ID, newTags);
        final Set<String> finalTags = this.appService.getTagsForApplication(APP_1_ID);
        Assertions.assertThat(finalTags).hasSize(3).containsAll(newTags);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetTagsForApplication() throws GenieException {
        Assertions.assertThat(this.appService.getTagsForApplication(APP_1_ID)).hasSize(1);
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testRemoveAllTagsForApplication() throws GenieException {
        Assertions.assertThat(this.appService.getTagsForApplication(APP_1_ID)).hasSize(1);
        this.appService.removeAllTagsForApplication(APP_1_ID);
        Assertions.assertThat(this.appService.getTagsForApplication(APP_1_ID)).isEmpty();
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testRemoveTagForApplication() throws GenieException {
        final Set<String> tags = this.appService.getTagsForApplication(APP_1_ID);
        Assertions.assertThat(tags).contains("prod");
        this.appService.removeTagForApplication(APP_1_ID, "prod");
        Assertions.assertThat(this.appService.getTagsForApplication(APP_1_ID)).doesNotContain("prod");
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/init.xml")
    void testGetCommandsForApplication() throws GenieException {
        final Set<Command> commands = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assertions
            .assertThat(commands).hasSize(1)
            .hasOnlyOneElementSatisfying(command -> Assertions.assertThat(command.getId()).isEqualTo(COMMAND_1_ID));
    }

    @Test
    void testGetCommandsForApplicationNoId() {
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.appService.getCommandsForApplication("", null));
    }

    @Test
    @DatabaseSetup("JpaApplicationPersistenceServiceImplIntegrationTest/deleteUnusedApplications/before.xml")
        /*
         * Unfortunately this doesn't seem to work due to how we have an after each that deletes the records out of
         * database before this check can happen. I don't want to break all the other tests but leaving for now as
         * might be a nice way to do it without so many java assertions
         */
//    @ExpectedDatabase(
//        value = "JpaApplicationPersistenceServiceImplIntegrationTest/deleteUnusedApplications/after.xml",
//        assertionMode = DatabaseAssertionMode.NON_STRICT
//    )
    void testDeleteUnusedApplications() {
        Assertions.assertThat(this.applicationRepository.existsByUniqueId("app2")).isTrue();
        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(6);
        final Instant createdThreshold = Instant.parse("2020-03-10T02:44:00.000Z");
        Assertions.assertThat(this.appService.deleteUnusedApplicationsCreatedBefore(createdThreshold)).isEqualTo(1L);
        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(5);
        Assertions.assertThat(this.applicationRepository.existsByUniqueId("app2")).isFalse();
    }
}
