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
package com.netflix.genie.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.client.apis.SortAttribute;
import com.netflix.genie.client.apis.SortDirection;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for {@link ApplicationClient}.
 *
 * @author amsharma
 */
abstract class ApplicationClientIntegrationTest extends GenieClientIntegrationTestBase {

    @Test
    void testCanCreateAndGetApplication() throws Exception {
        final String id = UUID.randomUUID().toString();
        final Application application = this.constructApplicationDTO(id);

        final String applicationId = this.applicationClient.createApplication(application);
        Assertions.assertThat(applicationId).isEqualTo(id);

        // Make sure Genie Not Found Exception is not thrown for this call.
        final Application app2 = this.applicationClient.getApplication(id);

        // Make sure the object returned is exactly what was sent to be created
        Assertions.assertThat(application.getId()).isEqualTo(app2.getId());
        Assertions.assertThat(application.getName()).isEqualTo(app2.getName());
        Assertions.assertThat(application.getDescription()).isEqualTo(app2.getDescription());
        Assertions.assertThat(application.getConfigs()).isEqualTo(app2.getConfigs());
        Assertions.assertThat(application.getSetupFile()).isEqualTo(app2.getSetupFile());
        Assertions.assertThat(application.getTags()).contains("foo", "bar");
        Assertions.assertThat(application.getStatus()).isEqualTo(app2.getStatus());
    }

    @Test
    void testGetApplicationsUsingParams() throws Exception {
        final String application1Id = UUID.randomUUID().toString();
        final String application2Id = UUID.randomUUID().toString();

        final Set<String> application1Tags = Sets.newHashSet("foo", "pi");
        final Set<String> application2Tags = Sets.newHashSet("bar", "pi");

        final Application application1 = new Application.Builder(
            "application1name",
            "application1user",
            "1.0",
            ApplicationStatus.ACTIVE
        )
            .withId(application1Id)
            .withTags(application1Tags)
            .build();

        final Application application2 =
            new Application.Builder(
                "application2name",
                "application2user",
                "2.0",
                ApplicationStatus.INACTIVE
            )
                .withId(application2Id)
                .withTags(application2Tags)
                .build();

        this.applicationClient.createApplication(application1);
        this.applicationClient.createApplication(application2);

        // Test get by tags
        List<Application> applicationList = this.applicationClient.getApplications(
            null,
            null,
            null,
            Lists.newArrayList("foo"),
            null
        );
        Assertions
            .assertThat(applicationList)
            .hasSize(1)
            .extracting(Application::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(application1Id);

        applicationList = this.applicationClient.getApplications(
            null,
            null,
            null,
            Lists.newArrayList("pi"),
            null
        );

        Assertions
            .assertThat(applicationList)
            .hasSize(2)
            .extracting(Application::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(application2Id, application1Id);

        // Test get by name
        applicationList = this.applicationClient.getApplications(
            "application1name",
            null,
            null,
            null,
            null
        );

        Assertions
            .assertThat(applicationList)
            .hasSize(1)
            .extracting(Application::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(application1Id);

        // Test get by status
        applicationList = this.applicationClient.getApplications(
            null,
            null,
            Lists.newArrayList(ApplicationStatus.ACTIVE.toString()),
            null,
            null
        );

        Assertions
            .assertThat(applicationList)
            .hasSize(1)
            .extracting(Application::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(application1Id);

        applicationList = this.applicationClient.getApplications(
            null,
            null,
            Arrays.asList(ApplicationStatus.ACTIVE.toString(), ApplicationStatus.INACTIVE.toString()),
            null,
            null
        );

        Assertions
            .assertThat(applicationList)
            .hasSize(2)
            .extracting(Application::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactlyInAnyOrder(application1Id, application2Id);
    }

    @Test
    void testGetApplicationsUsingPagination() throws Exception {
        final String id1 = UUID.randomUUID().toString() + "_1";
        final String id2 = UUID.randomUUID().toString() + "_2";
        final String id3 = UUID.randomUUID().toString() + "_3";

        final List<String> ids = Lists.newArrayList(id1, id2, id3);

        for (final String id : ids) {
            final Application application = new Application.Builder(
                "ApplicationName",
                "user",
                "1.0",
                ApplicationStatus.ACTIVE
            )
                .withId(id)
                .withTags(Sets.newHashSet("foo", "bar"))
                .build();
            this.applicationClient.createApplication(application);
        }

        final List<Application> results = this.applicationClient.getApplications(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        Assertions.assertThat(results).hasSize(3);
        Assertions.assertThat(
            results.stream()
                .map(Application::getId)
                .map(Optional::get)
        ).containsExactlyInAnyOrder(id1, id2, id3);

        // Paginate, 1 result per page
        for (int i = 0; i < ids.size(); i++) {
            final List<Application> page = this.applicationClient.getApplications(
                null,
                null,
                null,
                null,
                null,
                1,
                SortAttribute.UPDATED,
                SortDirection.ASC,
                i
            );

            Assertions.assertThat(page.size()).isEqualTo(1);
            Assertions.assertThat(page.get(0).getId().orElse(null)).isEqualTo(ids.get(i));
        }

        // Paginate, 1 result per page, reverse order
        Collections.reverse(ids);
        for (int i = 0; i < ids.size(); i++) {
            final List<Application> page = this.applicationClient.getApplications(
                null,
                null,
                null,
                null,
                null,
                1,
                SortAttribute.UPDATED,
                SortDirection.DESC,
                i
            );

            Assertions.assertThat(page.size()).isEqualTo(1);
            Assertions.assertThat(page.get(0).getId().orElse(null)).isEqualTo(ids.get(i));
        }

        // Ask for page beyond end of results
        Assertions.assertThat(
            this.applicationClient.getApplications(
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                1
            )
        ).isEmpty();
    }

    @Test
    void testApplicationNotExist() {
        Assertions.assertThatIOException().isThrownBy(() -> this.applicationClient.getApplication("foo"));
    }

    @Test
    void testGetAllAndDeleteAllApplications() throws Exception {
        final List<Application> initialApplicationList = this.applicationClient.getApplications();
        Assertions.assertThat(initialApplicationList).isEmpty();

        final Application application1 = constructApplicationDTO(null);
        final Application application2 = constructApplicationDTO(null);

        final String app1Id = this.applicationClient.createApplication(application1);
        final String app2Id = this.applicationClient.createApplication(application2);

        final List<Application> finalApplicationList = this.applicationClient.getApplications();
        Assertions.assertThat(finalApplicationList).hasSize(2)
            .extracting(Application::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(app2Id, app1Id);

        this.applicationClient.deleteAllApplications();
        Assertions.assertThat(this.applicationClient.getApplications()).isEmpty();
    }

    @Test
    void testDeleteApplication() throws Exception {
        final Application application1 = constructApplicationDTO(null);
        final String appId1 = this.applicationClient.createApplication(application1);

        final Application application2 = this.applicationClient.getApplication(appId1);
        Assertions.assertThat(application2.getId()).isPresent().contains(appId1);

        this.applicationClient.deleteApplication(appId1);
        Assertions.assertThatIOException().isThrownBy(() -> this.applicationClient.getApplication(appId1));
    }

    @Test
    void testUpdateApplication() throws Exception {
        final Application application1 = constructApplicationDTO(null);
        final String app1Id = this.applicationClient.createApplication(application1);

        final Application application2 = this.applicationClient.getApplication(app1Id);
        Assertions.assertThat(application2.getName()).isEqualTo(application1.getName());

        final Application application3 = new Application.Builder(
            "newname",
            "newuser",
            "new version",
            ApplicationStatus.ACTIVE
        )
            .withId(app1Id)
            .build();

        this.applicationClient.updateApplication(app1Id, application3);

        final Application application4 = this.applicationClient.getApplication(app1Id);

        Assertions.assertThat(application4.getName()).isEqualTo("newname");
        Assertions.assertThat(application4.getUser()).isEqualTo("newuser");
        Assertions.assertThat(application4.getVersion()).isEqualTo("new version");
        Assertions.assertThat(application4.getStatus()).isEqualByComparingTo(ApplicationStatus.ACTIVE);
        Assertions.assertThat(application4.getSetupFile()).isNotPresent();
        Assertions.assertThat(application4.getDescription()).isNotPresent();
        Assertions.assertThat(application4.getConfigs()).isEmpty();
        Assertions.assertThat(application4.getTags()).doesNotContain("foo");
    }

    @Test
    void testApplicationTagsMethods() throws Exception {
        final Set<String> initialTags = Sets.newHashSet("foo", "bar");

        final Application application = new Application.Builder("name", "user", "1.0", ApplicationStatus.ACTIVE)
            .withTags(initialTags)
            .build();

        final String appId = this.applicationClient.createApplication(application);

        // Test getTags for application
        Assertions
            .assertThat(this.applicationClient.getTagsForApplication(appId))
            .hasSize(4)
            .contains("foo", "bar");

        // Test adding a tag for application
        final Set<String> moreTags = Sets.newHashSet("pi");

        this.applicationClient.addTagsToApplication(appId, moreTags);
        Assertions
            .assertThat(this.applicationClient.getTagsForApplication(appId))
            .hasSize(5)
            .contains("foo", "bar", "pi");

        // Test removing a tag for application
        this.applicationClient.removeTagFromApplication(appId, "bar");
        Assertions
            .assertThat(this.applicationClient.getTagsForApplication(appId))
            .hasSize(4)
            .doesNotContain("bar")
            .contains("foo", "pi");

        // Test update tags for a application
        this.applicationClient.updateTagsForApplication(appId, initialTags);
        Assertions
            .assertThat(this.applicationClient.getTagsForApplication(appId))
            .hasSize(4)
            .contains("foo", "bar");

        // Test delete all tags in a application
        this.applicationClient.removeAllTagsForApplication(appId);
        Assertions
            .assertThat(this.applicationClient.getTagsForApplication(appId))
            .hasSize(2)
            .doesNotContain("foo", "bar");
    }

    @Test
    void testApplicationConfigsMethods() throws Exception {
        final Set<String> initialConfigs = Sets.newHashSet("foo", "bar");

        final Application application = new Application.Builder("name", "user", "1.0", ApplicationStatus.ACTIVE)
            .withConfigs(initialConfigs)
            .build();

        final String appId = this.applicationClient.createApplication(application);

        // Test getConfigs for application
        Assertions
            .assertThat(this.applicationClient.getConfigsForApplication(appId))
            .hasSize(2)
            .contains("foo", "bar");

        // Test adding a config to the application
        final Set<String> moreConfigs = Sets.newHashSet("pi");
        this.applicationClient.addConfigsToApplication(appId, moreConfigs);
        Assertions
            .assertThat(this.applicationClient.getConfigsForApplication(appId))
            .hasSize(3)
            .contains("foo", "bar", "pi");

        // Test update configs for an application
        this.applicationClient.updateConfigsForApplication(appId, initialConfigs);
        Assertions
            .assertThat(this.applicationClient.getConfigsForApplication(appId))
            .hasSize(2)
            .contains("foo", "bar");

        // Test delete all configs for an application
        this.applicationClient.removeAllConfigsForApplication(appId);
        Assertions.assertThat(this.applicationClient.getConfigsForApplication(appId)).isEmpty();
    }

    @Test
    void testApplicationDependenciesMethods() throws Exception {
        final Set<String> initialDependencies = Sets.newHashSet("foo", "bar");

        final Application application = new Application.Builder("name", "user", "1.0", ApplicationStatus.ACTIVE)
            .withDependencies(initialDependencies)
            .build();

        final String appId = this.applicationClient.createApplication(application);

        // Test getDependencies for application
        Assertions
            .assertThat(this.applicationClient.getDependenciesForApplication(appId))
            .hasSize(2)
            .contains("foo", "bar");

        // Test adding a dependency to the application
        final Set<String> moreDependencies = Sets.newHashSet("pi");
        this.applicationClient.addDependenciesToApplication(appId, moreDependencies);
        Assertions
            .assertThat(this.applicationClient.getDependenciesForApplication(appId))
            .hasSize(3)
            .contains("foo", "bar", "pi");

        // Test update dependencies for an application
        this.applicationClient.updateDependenciesForApplication(appId, initialDependencies);
        Assertions
            .assertThat(this.applicationClient.getDependenciesForApplication(appId))
            .hasSize(2)
            .contains("foo", "bar");

        // Test delete all dependencies for an application
        this.applicationClient.removeAllDependenciesForApplication(appId);
        Assertions.assertThat(this.applicationClient.getDependenciesForApplication(appId)).isEmpty();
    }

    @Test
    void testApplicationPatchMethod() throws Exception {
        final ObjectMapper mapper = GenieObjectMapper.getMapper();
        final String newName = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + newName + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(mapper.readTree(patchString));

        final Application application = this.constructApplicationDTO("application1");

        final String appId = this.applicationClient.createApplication(application);
        this.applicationClient.patchApplication(appId, patch);

        Assertions
            .assertThat(this.applicationClient.getApplication(appId))
            .extracting(Application::getName)
            .isEqualTo(newName);
    }

    @Test
    void testCanGetCommandsForApplication() throws Exception {
        final Command command1 = constructCommandDTO(null);
        final Command command2 = constructCommandDTO(null);

        final String command1Id = this.commandClient.createCommand(command1);
        final String command2Id = this.commandClient.createCommand(command2);

        final Application application = constructApplicationDTO(null);

        final String appId = this.applicationClient.createApplication(application);

        this.commandClient.addApplicationsToCommand(command1Id, Lists.newArrayList(appId));
        this.commandClient.addApplicationsToCommand(command2Id, Lists.newArrayList(appId));

        Assertions
            .assertThat(this.applicationClient.getCommandsForApplication(appId))
            .hasSize(2)
            .extracting(Command::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .contains(command1Id, command2Id);
    }
}
