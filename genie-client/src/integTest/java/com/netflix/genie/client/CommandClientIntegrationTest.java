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
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Integration Tests for {@link CommandClient}.
 *
 * @author amsharma
 */
abstract class CommandClientIntegrationTest extends ApplicationClientIntegrationTest {

    @Test
    void testCanCreateAndGetCommand() throws Exception {
        final String id = UUID.randomUUID().toString();
        final Command command = this.constructCommandDTO(id);

        final String commandId = this.commandClient.createCommand(command);
        Assertions.assertThat(commandId).isEqualTo(id);

        // Make sure Genie Not Found Exception is not thrown for this call.
        final Command cmd = this.commandClient.getCommand(id);

        // Make sure the object returned is exactly what was sent to be created
        Assertions.assertThat(cmd.getId()).isPresent().contains(id);
        Assertions.assertThat(cmd.getName()).isEqualTo(command.getName());
        Assertions.assertThat(cmd.getDescription()).isEqualTo(command.getDescription());
        Assertions.assertThat(cmd.getConfigs()).isEqualTo(command.getConfigs());
        Assertions.assertThat(cmd.getSetupFile()).isEqualTo(command.getSetupFile());
        Assertions.assertThat(cmd.getTags()).contains("foo", "bar");
        Assertions.assertThat(cmd.getStatus()).isEqualByComparingTo(command.getStatus());
    }

    @Test
    void testGetCommandsUsingParams() throws Exception {
        final Set<String> command1Tags = Sets.newHashSet("foo", "pi");
        final Set<String> command2Tags = Sets.newHashSet("bar", "pi");

        final List<String> executableAndArgs = Lists.newArrayList("exec");

        final Command command1 = new Command.Builder(
            "command1name",
            "command1user",
            "1.0",
            CommandStatus.ACTIVE,
            executableAndArgs,
            1000
        )
            .withTags(command1Tags)
            .build();

        final Command command2 =
            new Command.Builder(
                "command2name",
                "command2user",
                "2.0",
                CommandStatus.INACTIVE,
                executableAndArgs,
                1000
            )
                .withTags(command2Tags)
                .build();

        final String command1Id = this.commandClient.createCommand(command1);
        final String command2Id = this.commandClient.createCommand(command2);

        // Test get by tags
        Assertions
            .assertThat(this.commandClient.getCommands(null, null, null, Lists.newArrayList("foo")))
            .hasSize(1)
            .extracting(Command::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(command1Id);

        Assertions
            .assertThat(this.commandClient.getCommands(null, null, null, Lists.newArrayList("pi")))
            .hasSize(2)
            .extracting(Command::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(command2Id, command1Id);

        // Test get by name
        Assertions
            .assertThat(this.commandClient.getCommands("command1name", null, null, null))
            .hasSize(1)
            .extracting(Command::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(command1Id);

        // Test get by status
        Assertions
            .assertThat(
                this.commandClient.getCommands(null, null, Lists.newArrayList(CommandStatus.ACTIVE.toString()), null)
            )
            .hasSize(1)
            .extracting(Command::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(command1Id);

        final List<String> statuses = Lists.newArrayList(
            CommandStatus.ACTIVE.toString(),
            CommandStatus.INACTIVE.toString()
        );
        Assertions
            .assertThat(this.commandClient.getCommands(null, null, statuses, null))
            .hasSize(2)
            .extracting(Command::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(command2Id, command1Id);

        // Test find by user
        Assertions
            .assertThat(this.commandClient.getCommands(null, "command2user", null, null))
            .hasSize(1)
            .extracting(Command::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(command2Id);
    }

    @Test
    void testGetCommandsUsingPagination() throws Exception {
        final String id1 = UUID.randomUUID().toString() + "_1";
        final String id2 = UUID.randomUUID().toString() + "_2";
        final String id3 = UUID.randomUUID().toString() + "_3";

        final List<String> ids = Lists.newArrayList(id1, id2, id3);

        for (final String id : ids) {
            final Command command = new Command.Builder(
                "ClusterName",
                "user",
                "1.0",
                CommandStatus.ACTIVE,
                Lists.newArrayList("echo"),
                1000
            )
                .withId(id)
                .withTags(Sets.newHashSet("foo", "bar"))
                .build();
            this.commandClient.createCommand(command);
        }

        final List<Command> results = this.commandClient.getCommands(
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
                .map(Command::getId)
                .map(Optional::get)
        ).containsExactlyInAnyOrder(id1, id2, id3);

        // Paginate, 1 result per page
        for (int i = 0; i < ids.size(); i++) {
            final List<Command> page = this.commandClient.getCommands(
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
            final List<Command> page = this.commandClient.getCommands(
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
            this.commandClient.getCommands(
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
    void testCommandNotExist() {
        Assertions
            .assertThatIOException()
            .isThrownBy(() -> this.commandClient.getCommand(UUID.randomUUID().toString()));
    }

    @Test
    void testGetAllAndDeleteAllCommands() throws Exception {
        Assertions.assertThat(this.commandClient.getCommands()).isEmpty();

        final Command command1 = this.constructCommandDTO(null);
        final Command command2 = this.constructCommandDTO(null);

        final String command1Id = this.commandClient.createCommand(command1);
        final String command2Id = this.commandClient.createCommand(command2);

        Assertions
            .assertThat(this.commandClient.getCommands())
            .hasSize(2)
            .extracting(Command::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(command2Id, command1Id);

        this.commandClient.deleteAllCommands();
        Assertions.assertThat(this.commandClient.getCommands()).isEmpty();
    }

    @Test
    void testDeleteCommand() throws Exception {
        final Command command = this.constructCommandDTO(null);
        final String commandId = this.commandClient.createCommand(command);

        Assertions.assertThat(this.commandClient.getCommand(commandId).getId()).isPresent().contains(commandId);

        this.commandClient.deleteCommand(commandId);
        Assertions.assertThatIOException().isThrownBy(() -> this.commandClient.getCommand(commandId));
    }

    @Test
    void testUpdateCommand() throws Exception {
        final Command command1 = this.constructCommandDTO(null);
        final String commandId = this.commandClient.createCommand(command1);

        final Command command2 = this.commandClient.getCommand(commandId);
        Assertions.assertThat(command2.getId()).isPresent().contains(commandId);

        final List<String> executableAndArgs = Lists.newArrayList("exec");

        final Command command3 = new Command.Builder(
            "newname",
            "newuser",
            "new version",
            CommandStatus.ACTIVE,
            executableAndArgs,
            1000
        )
            .withId(commandId)
            .build();

        this.commandClient.updateCommand(commandId, command3);

        final Command command4 = this.commandClient.getCommand(commandId);

        Assertions.assertThat(command4.getName()).isEqualTo("newname");
        Assertions.assertThat(command4.getUser()).isEqualTo("newuser");
        Assertions.assertThat(command4.getVersion()).isEqualTo("new version");
        Assertions.assertThat(command4.getExecutableAndArguments()).isEqualTo(executableAndArgs);
        Assertions.assertThat(command4.getStatus()).isEqualByComparingTo(CommandStatus.ACTIVE);
        Assertions.assertThat(command4.getSetupFile()).isNotPresent();
        Assertions.assertThat(command4.getDescription()).isNotPresent();
        Assertions.assertThat(command4.getConfigs()).isEmpty();
        Assertions.assertThat(command4.getTags()).doesNotContain("foo");
    }

    @Test
    void testCommandTagsMethods() throws Exception {
        final Set<String> initialTags = Sets.newHashSet("foo", "bar");
        final List<String> executable = Lists.newArrayList("exec");

        final Command command = new Command.Builder("name", "user", "1.0", CommandStatus.ACTIVE, executable, 1000)
            .withTags(initialTags)
            .build();

        final String commandId = this.commandClient.createCommand(command);

        // Test getTags for command
        Assertions.assertThat(this.commandClient.getTagsForCommand(commandId)).hasSize(4).contains("foo", "bar");

        // Test adding a tag for command
        final Set<String> moreTags = Sets.newHashSet("pi");

        this.commandClient.addTagsToCommand(commandId, moreTags);
        Assertions.assertThat(this.commandClient.getTagsForCommand(commandId)).hasSize(5).contains("foo", "bar", "pi");

        // Test removing a tag for command
        this.commandClient.removeTagFromCommand(commandId, "bar");
        Assertions.assertThat(this.commandClient.getTagsForCommand(commandId)).hasSize(4).contains("foo", "pi");

        // Test update tags for a command
        this.commandClient.updateTagsForCommand(commandId, initialTags);
        Assertions.assertThat(this.commandClient.getTagsForCommand(commandId)).hasSize(4).contains("foo", "bar");

        // Test delete all tags in a command
        this.commandClient.removeAllTagsForCommand(commandId);
        Assertions.assertThat(this.commandClient.getTagsForCommand(commandId)).hasSize(2);
    }

    @Test
    void testCommandConfigsMethods() throws Exception {
        final Set<String> initialConfigs = Sets.newHashSet("foo", "bar");
        final List<String> executable = Lists.newArrayList("exec");

        final Command command = new Command.Builder("name", "user", "1.0", CommandStatus.ACTIVE, executable, 1000)
            .withConfigs(initialConfigs)
            .build();

        final String commandId = this.commandClient.createCommand(command);

        // Test getConfigs for command
        Assertions.assertThat(this.commandClient.getConfigsForCommand(commandId)).hasSize(2).contains("foo", "bar");

        // Test adding a tag for command
        final Set<String> moreConfigs = Sets.newHashSet("pi");

        this.commandClient.addConfigsToCommand(commandId, moreConfigs);
        Assertions
            .assertThat(this.commandClient.getConfigsForCommand(commandId))
            .hasSize(3)
            .contains("foo", "bar", "pi");

        // Test update configs for a command
        this.commandClient.updateConfigsForCommand(commandId, initialConfigs);
        Assertions.assertThat(this.commandClient.getConfigsForCommand(commandId)).hasSize(2).contains("foo", "bar");

        // Test delete all configs in a command
        this.commandClient.removeAllConfigsForCommand(commandId);
        Assertions.assertThat(this.commandClient.getConfigsForCommand(commandId)).isEmpty();
    }

    @Test
    void testCommandDependenciesMethods() throws Exception {
        final Set<String> initialDependencies = Sets.newHashSet("foo", "bar");
        final List<String> executable = Lists.newArrayList("exec");

        final Command command = new Command.Builder("name", "user", "1.0", CommandStatus.ACTIVE, executable, 1000)
            .withDependencies(initialDependencies)
            .build();

        final String commandId = this.commandClient.createCommand(command);

        // Test getDependencies for command
        Assertions
            .assertThat(this.commandClient.getDependenciesForCommand(commandId))
            .hasSize(2)
            .contains("foo", "bar");

        // Test adding a tag for command
        final Set<String> moreDependencies = Sets.newHashSet("pi");

        this.commandClient.addDependenciesToCommand(commandId, moreDependencies);
        Assertions
            .assertThat(this.commandClient.getDependenciesForCommand(commandId))
            .hasSize(3)
            .contains("foo", "bar", "pi");

        // Test update configs for a command
        this.commandClient.updateDependenciesForCommand(commandId, initialDependencies);
        Assertions
            .assertThat(this.commandClient.getDependenciesForCommand(commandId))
            .hasSize(2)
            .contains("foo", "bar");

        // Test delete all configs in a command
        this.commandClient.removeAllDependenciesForCommand(commandId);
        Assertions.assertThat(this.commandClient.getDependenciesForCommand(commandId)).isEmpty();
    }

    @Test
    void testCommandApplicationsMethods() throws Exception {
        final Application foo = new Application.Builder(
            "name",
            "user",
            "version",
            ApplicationStatus.ACTIVE
        ).build();

        final String fooId = this.applicationClient.createApplication(foo);

        final Application bar = new Application.Builder(
            "name",
            "user",
            "version",
            ApplicationStatus.ACTIVE
        ).build();

        final String barId = this.applicationClient.createApplication(bar);

        final Application pi = new Application.Builder(
            "name",
            "user",
            "version",
            ApplicationStatus.ACTIVE
        ).build();

        final String piId = this.applicationClient.createApplication(pi);

        final Command command = this.constructCommandDTO("command1");

        final String commandId = this.commandClient.createCommand(command);

        // Test add Applications to command
        final List<String> initialApplications = Lists.newArrayList(fooId, barId, piId);

        this.commandClient.addApplicationsToCommand(commandId, initialApplications);

        Assertions
            .assertThat(this.commandClient.getApplicationsForCommand(commandId))
            .extracting(Application::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .isEqualTo(initialApplications);

        // Test removing a application for command
        this.commandClient.removeApplicationFromCommand(commandId, piId);
        Assertions
            .assertThat(this.commandClient.getApplicationsForCommand(commandId))
            .extracting(Application::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(fooId, barId);

        final List<String> updatedApplications = Lists.newArrayList(fooId, piId);

        // Test update applications for a command
        this.commandClient.updateApplicationsForCommand(commandId, updatedApplications);
        Assertions
            .assertThat(this.commandClient.getApplicationsForCommand(commandId))
            .extracting(Application::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly(fooId, piId);

        // Test delete all applications in a command
        this.commandClient.removeAllApplicationsForCommand(commandId);
        Assertions.assertThat(this.commandClient.getApplicationsForCommand(commandId)).isEmpty();
    }

    @Test
    void testCommandPatchMethod() throws Exception {
        final ObjectMapper mapper = GenieObjectMapper.getMapper();
        final String newName = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + newName + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(mapper.readTree(patchString));

        final Command command = this.constructCommandDTO(null);

        final String commandId = this.commandClient.createCommand(command);
        this.commandClient.patchCommand(commandId, patch);

        Assertions.assertThat(this.commandClient.getCommand(commandId).getName()).isEqualTo(newName);
    }

    @Test
    void testCanGetClustersForCommand() throws Exception {
        final Cluster cluster1 = this.constructClusterDTO(null);
        final Cluster cluster2 = this.constructClusterDTO(null);

        final String cluster1Id = this.clusterClient.createCluster(cluster1);
        final String cluster2Id = this.clusterClient.createCluster(cluster2);

        final Command command = new Command.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(), UUID.randomUUID().toString(),
            CommandStatus.ACTIVE,
            Lists.newArrayList(UUID.randomUUID().toString()),
            100L
        )
            .withClusterCriteria(
                Lists.newArrayList(
                    new Criterion.Builder().withId(cluster1Id).build(),
                    new Criterion.Builder().withId(cluster2Id).build()
                )
            )
            .build();

        final String commandId = this.commandClient.createCommand(command);

        Assertions
            .assertThat(this.commandClient.getClustersForCommand(commandId))
            .extracting(Cluster::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsOnly(cluster1Id, cluster2Id);
    }
}
