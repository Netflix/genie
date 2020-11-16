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
package com.netflix.genie.web.data.services.impl.jpa;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.ApplicationMetadata;
import com.netflix.genie.common.external.dtos.v4.ApplicationRequest;
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.validation.ConstraintViolationException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for {@link JpaPersistenceServiceImpl} focusing on application related functionality.
 *
 * @author tgianos
 * @since 2.0.0
 */
class JpaPersistenceServiceImplApplicationsIntegrationTest extends JpaPersistenceServiceIntegrationTestBase {

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

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplication() throws NotFoundException {
        final Application app = this.service.getApplication(APP_1_ID);
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

        final Application app2 = this.service.getApplication(APP_2_ID);
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

        final Application app3 = this.service.getApplication(APP_3_ID);
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
            .isThrownBy(() -> this.service.getApplication(""));
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsByName() {
        final Page<Application> apps = this.service.findApplications(APP_2_NAME, null, null, null, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_2_ID);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsByUser() {
        final Page<Application> apps = this.service.findApplications(null, APP_1_USER, null, null, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_1_ID);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsByStatuses() {
        final Set<ApplicationStatus> statuses = Sets.newHashSet(ApplicationStatus.ACTIVE, ApplicationStatus.INACTIVE);
        final Page<Application> apps = this.service.findApplications(null, null, statuses, null, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_1_ID);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsByTags() {
        final Set<String> tags = Sets.newHashSet("prod");
        Page<Application> apps = this.service.findApplications(null, null, null, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_1_ID);

        tags.add("yarn");
        apps = this.service.findApplications(null, null, null, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_2_ID);

        tags.add("somethingThatWouldNeverReallyExist");
        apps = this.service.findApplications(null, null, null, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(0);

        tags.clear();
        apps = this.service.findApplications(null, null, null, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_1_ID);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsByTagsWhenOneDoesntExist() {
        final Set<String> tags = Sets.newHashSet("prod", UUID.randomUUID().toString());
        final Page<Application> apps = this.service.findApplications(null, null, null, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(0);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsByType() {
        final Page<Application> apps = this.service.findApplications(null, null, null, null, APP_2_TYPE, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_2_ID);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsDescending() {
        //Default to order by Updated
        final Page<Application> apps = this.service.findApplications(null, null, null, null, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_1_ID);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsAscending() {
        //Default to order by Updated
        final Pageable ascendingPage = PageRequest.of(0, 10, Sort.Direction.ASC, "updated");
        final Page<Application> apps = this.service.findApplications(null, null, null, null, null, ascendingPage);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_1_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_3_ID);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsOrderBysDefault() {
        //Default to order by Updated
        final Page<Application> apps = this.service.findApplications(null, null, null, null, null, PAGEABLE);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_2_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_1_ID);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsOrderBysName() {
        final Pageable orderByNamePage = PageRequest.of(0, 10, Sort.Direction.DESC, "name");
        final Page<Application> apps = this.service.findApplications(null, null, null, null, null, orderByNamePage);
        Assertions.assertThat(apps.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_1_ID);
        Assertions.assertThat(apps.getContent().get(1).getId()).isEqualTo(APP_3_ID);
        Assertions.assertThat(apps.getContent().get(2).getId()).isEqualTo(APP_2_ID);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsOrderBysInvalidField() {
        final Pageable orderByInvalidPage = PageRequest.of(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
        Assertions
            .assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> this.service.findApplications(null, null, null, null, null, orderByInvalidPage));
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetApplicationsWithTags() {
        final Set<ApplicationStatus> inactiveStatuses = Sets.newHashSet(ApplicationStatus.INACTIVE);
        final Set<ApplicationStatus> activeStatuses = Sets.newHashSet(ApplicationStatus.ACTIVE);
        final Set<String> tags = Sets.newHashSet("prod", "yarn");
        Page<Application> apps;
        apps = this.service.findApplications(null, null, activeStatuses, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getTotalElements()).isEqualTo(1);
        Assertions.assertThat(apps.getContent().get(0).getId()).isEqualTo(APP_2_ID);
        apps = this.service.findApplications(null, null, inactiveStatuses, tags, null, PAGEABLE);
        Assertions.assertThat(apps.getTotalElements()).isEqualTo(0);
    }

    @Test
    void testCreateApplication() throws GenieCheckedException {
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
        final String createdId = this.service.saveApplication(app);
        Assertions.assertThat(createdId).isEqualTo(id);
        final Application created = this.service.getApplication(id);
        Assertions.assertThat(created.getId()).isEqualTo(createdId);
        final ApplicationMetadata appMetadata = created.getMetadata();
        Assertions.assertThat(appMetadata.getName()).isEqualTo(APP_1_NAME);
        Assertions.assertThat(appMetadata.getUser()).isEqualTo(APP_1_USER);
        Assertions.assertThat(appMetadata.getStatus()).isEqualByComparingTo(ApplicationStatus.ACTIVE);
        this.service.deleteApplication(id);
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getApplication(id));
    }

    @Test
    void testCreateApplicationNoId() throws GenieCheckedException {
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
        final String id = this.service.saveApplication(app);
        final Application created = this.service.getApplication(id);
        final ApplicationMetadata appMetadata = created.getMetadata();
        Assertions.assertThat(appMetadata.getName()).isEqualTo(APP_1_NAME);
        Assertions.assertThat(appMetadata.getUser()).isEqualTo(APP_1_USER);
        Assertions.assertThat(appMetadata.getStatus()).isEqualByComparingTo(ApplicationStatus.ACTIVE);
        this.service.deleteApplication(id);
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getApplication(id));
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testUpdateApplication() throws GenieCheckedException {
        final Application getApp = this.service.getApplication(APP_1_ID);
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

        this.service.updateApplication(APP_1_ID, updateApp);

        final Application updated = this.service.getApplication(APP_1_ID);
        Assertions.assertThat(updated.getUpdated()).isNotEqualTo(updateTime);
        Assertions.assertThat(updated.getMetadata().getUser()).isEqualTo(APP_2_USER);
        Assertions.assertThat(updated.getMetadata().getStatus()).isEqualByComparingTo(ApplicationStatus.ACTIVE);
        Assertions.assertThat(updated.getMetadata().getTags()).isEqualTo(tags);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testUpdateCreateAndUpdate() throws GenieCheckedException {
        final Application init = this.service.getApplication(APP_1_ID);
        final Instant created = init.getCreated();
        final Instant updated = init.getUpdated();

        this.service.updateApplication(APP_1_ID, init);

        final Application updatedApp = this.service.getApplication(APP_1_ID);
        Assertions.assertThat(updatedApp.getCreated()).isEqualTo(created);
        Assertions.assertThat(updatedApp.getUpdated()).isNotEqualTo(updated).isNotEqualTo(Instant.EPOCH);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testDeleteAll() throws GenieCheckedException {
        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(3L);
        // To solve referential integrity problem
        this.commandRepository.findByUniqueId(COMMAND_1_ID).ifPresent(this.commandRepository::delete);
        this.commandRepository.flush();
        this.service.deleteAllApplications();
        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(0L);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testDelete() throws GenieCheckedException {
        this.service.deleteApplication(APP_3_ID);
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getApplication(APP_3_ID));
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testAddConfigsToApplication() throws GenieCheckedException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assertions.assertThat(this.service.getConfigsForResource(APP_1_ID, Application.class)).hasSize(2);
        this.service.addConfigsToResource(APP_1_ID, newConfigs, Application.class);
        final Set<String> finalConfigs = this.service.getConfigsForResource(APP_1_ID, Application.class);
        Assertions.assertThat(finalConfigs).hasSize(5).containsAll(newConfigs);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testUpdateConfigsForApplication() throws GenieCheckedException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assertions.assertThat(this.service.getConfigsForResource(APP_1_ID, Application.class)).hasSize(2);
        this.service.updateConfigsForResource(APP_1_ID, newConfigs, Application.class);
        final Set<String> finalConfigs = this.service.getConfigsForResource(APP_1_ID, Application.class);
        Assertions.assertThat(finalConfigs).hasSize(3).containsAll(newConfigs);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetConfigsForApplication() throws GenieCheckedException {
        Assertions.assertThat(this.service.getConfigsForResource(APP_1_ID, Application.class)).hasSize(2);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testRemoveAllConfigsForApplication() throws GenieCheckedException {
        Assertions.assertThat(this.service.getConfigsForResource(APP_1_ID, Application.class)).hasSize(2);
        this.service.removeAllConfigsForResource(APP_1_ID, Application.class);
        Assertions.assertThat(this.service.getConfigsForResource(APP_1_ID, Application.class)).isEmpty();
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testRemoveConfigForApplication() throws GenieCheckedException {
        final Set<String> configs = this.service.getConfigsForResource(APP_1_ID, Application.class);
        Assertions.assertThat(configs).hasSize(2);
        final String removedConfig = configs.iterator().next();
        this.service.removeConfigForResource(APP_1_ID, removedConfig, Application.class);
        Assertions
            .assertThat(this.service.getConfigsForResource(APP_1_ID, Application.class))
            .doesNotContain(removedConfig);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testAddDependenciesToApplication() throws GenieCheckedException {
        final String newDependency1 = UUID.randomUUID().toString();
        final String newDependency2 = UUID.randomUUID().toString();
        final String newDependency3 = UUID.randomUUID().toString();

        final Set<String> newDependencies = Sets.newHashSet(newDependency1, newDependency2, newDependency3);

        Assertions.assertThat(this.service.getDependenciesForResource(APP_1_ID, Application.class)).hasSize(2);
        this.service.addDependenciesToResource(APP_1_ID, newDependencies, Application.class);
        final Set<String> finalDependencies = this.service.getDependenciesForResource(APP_1_ID, Application.class);
        Assertions.assertThat(finalDependencies).hasSize(5).containsAll(newDependencies);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testUpdateDependenciesForApplication() throws GenieCheckedException {
        final String newDependency1 = UUID.randomUUID().toString();
        final String newDependency2 = UUID.randomUUID().toString();
        final String newDependency3 = UUID.randomUUID().toString();

        final Set<String> newDependencies = Sets.newHashSet(newDependency1, newDependency2, newDependency3);

        Assertions.assertThat(this.service.getDependenciesForResource(APP_1_ID, Application.class)).hasSize(2);
        this.service.updateDependenciesForResource(APP_1_ID, newDependencies, Application.class);
        final Set<String> finalDependencies = this.service.getDependenciesForResource(APP_1_ID, Application.class);
        Assertions.assertThat(finalDependencies).hasSize(3).containsAll(newDependencies);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetDependenciesForApplication() throws GenieCheckedException {
        Assertions.assertThat(this.service.getDependenciesForResource(APP_1_ID, Application.class)).hasSize(2);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testRemoveAllDependenciesForApplication() throws GenieCheckedException {
        Assertions.assertThat(this.service.getDependenciesForResource(APP_1_ID, Application.class)).hasSize(2);
        this.service.removeAllDependenciesForResource(APP_1_ID, Application.class);
        Assertions.assertThat(this.service.getDependenciesForResource(APP_1_ID, Application.class)).isEmpty();
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testRemoveDependencyForApplication() throws GenieCheckedException {
        final Set<String> dependencies = this.service.getDependenciesForResource(APP_1_ID, Application.class);
        Assertions.assertThat(dependencies).hasSize(2);
        final String removedDependency = dependencies.iterator().next();
        this.service.removeDependencyForResource(APP_1_ID, removedDependency, Application.class);
        Assertions
            .assertThat(this.service.getDependenciesForResource(APP_1_ID, Application.class))
            .doesNotContain(removedDependency);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testAddTagsToApplication() throws GenieCheckedException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assertions.assertThat(this.service.getTagsForResource(APP_1_ID, Application.class)).hasSize(1);
        this.service.addTagsToResource(APP_1_ID, newTags, Application.class);
        final Set<String> finalTags = this.service.getTagsForResource(APP_1_ID, Application.class);
        Assertions.assertThat(finalTags).hasSize(4).containsAll(newTags);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testUpdateTagsForApplication() throws GenieCheckedException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assertions.assertThat(this.service.getTagsForResource(APP_1_ID, Application.class)).hasSize(1);
        this.service.updateTagsForResource(APP_1_ID, newTags, Application.class);
        final Set<String> finalTags = this.service.getTagsForResource(APP_1_ID, Application.class);
        Assertions.assertThat(finalTags).hasSize(3).containsAll(newTags);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetTagsForApplication() throws GenieCheckedException {
        Assertions.assertThat(this.service.getTagsForResource(APP_1_ID, Application.class)).hasSize(1);
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testRemoveAllTagsForApplication() throws GenieCheckedException {
        Assertions.assertThat(this.service.getTagsForResource(APP_1_ID, Application.class)).hasSize(1);
        this.service.removeAllTagsForResource(APP_1_ID, Application.class);
        Assertions.assertThat(this.service.getTagsForResource(APP_1_ID, Application.class)).isEmpty();
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testRemoveTagForApplication() throws GenieCheckedException {
        final Set<String> tags = this.service.getTagsForResource(APP_1_ID, Application.class);
        Assertions.assertThat(tags).contains("prod");
        this.service.removeTagForResource(APP_1_ID, "prod", Application.class);
        Assertions.assertThat(this.service.getTagsForResource(APP_1_ID, Application.class)).doesNotContain("prod");
    }

    @Test
    @DatabaseSetup("persistence/applications/init.xml")
    void testGetCommandsForApplication() throws GenieCheckedException {
        final Set<Command> commands = this.service.getCommandsForApplication(APP_1_ID, null);
        Assertions
            .assertThat(commands)
            .hasSize(1)
            .hasOnlyOneElementSatisfying(command -> Assertions.assertThat(command.getId()).isEqualTo(COMMAND_1_ID));
    }

    @Test
    void testGetCommandsForApplicationNoId() {
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.service.getCommandsForApplication("", null));
    }

    @Test
    @DatabaseSetup("persistence/applications/deleteUnusedApplications/before.xml")
    @ExpectedDatabase(
        value = "persistence/applications/deleteUnusedApplications/after.xml",
        assertionMode = DatabaseAssertionMode.NON_STRICT
    )
    void testDeleteUnusedApplications() {
        Assertions.assertThat(this.applicationRepository.existsByUniqueId("app2")).isTrue();
        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(6);
        final Instant createdThreshold = Instant.parse("2020-03-10T02:44:00.000Z");
        Assertions.assertThat(this.service.deleteUnusedApplications(createdThreshold, 10)).isEqualTo(1L);
        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(5);
        Assertions.assertThat(this.applicationRepository.existsByUniqueId("app2")).isFalse();
    }
}
