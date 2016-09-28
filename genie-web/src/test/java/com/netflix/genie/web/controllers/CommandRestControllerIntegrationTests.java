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
package com.netflix.genie.web.controllers;

import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import com.netflix.genie.web.hateoas.resources.CommandResource;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Integration tests for the Commands REST API.
 *
 * @author tgianos
 * @since 3.0.0
 */
//TODO: Add tests for error conditions
public class CommandRestControllerIntegrationTests extends RestControllerIntegrationTestsBase {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = "hive";
    private static final String USER = "genie";
    private static final String VERSION = "1.0.0";
    private static final String EXECUTABLE = "/apps/hive/bin/hive";
    private static final long CHECK_DELAY = 10000L;

    private static final String EXECUTABLE_PATH = "$.executable";
    private static final String CHECK_DELAY_PATH = "$.checkDelay";
    private static final String COMMANDS_LIST_PATH = EMBEDDED_PATH + ".commandList";

    @Autowired
    private JpaApplicationRepository jpaApplicationRepository;

    @Autowired
    private JpaClusterRepository jpaClusterRepository;

    @Autowired
    private JpaCommandRepository jpaCommandRepository;

    /**
     * Cleanup after tests.
     */
    @After
    public void cleanup() {
        this.jpaClusterRepository.deleteAll();
        this.jpaCommandRepository.deleteAll();
        this.jpaApplicationRepository.deleteAll();
    }

    /**
     * Test creating a command without an ID.
     *
     * @throws Exception on configuration issue
     */
    @Test
    public void canCreateCommandWithoutId() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        final String id = this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY).build(),
            null
        );

        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API + "/" + id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(id)))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(CommandStatus.ACTIVE.toString())))
            .andExpect(MockMvcResultMatchers.jsonPath(EXECUTABLE_PATH, Matchers.is(EXECUTABLE)))
            .andExpect(MockMvcResultMatchers.jsonPath(CHECK_DELAY_PATH, Matchers.is((int) CHECK_DELAY)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH + ".*", Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(CLUSTERS_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(APPLICATIONS_LINK_KEY)));

        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(1L));
    }

    /**
     * Test creating a Command with an ID.
     *
     * @throws Exception When issue in creation
     */
    @Test
    public void canCreateCommandWithId() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY).withId(ID).build(),
            null
        );

        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API + "/" + ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(ID)))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(CommandStatus.ACTIVE.toString())))
            .andExpect(MockMvcResultMatchers.jsonPath(EXECUTABLE_PATH, Matchers.is(EXECUTABLE)))
            .andExpect(MockMvcResultMatchers.jsonPath(CHECK_DELAY_PATH, Matchers.is((int) CHECK_DELAY)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.id:" + ID)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH + ".*", Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(CLUSTERS_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(APPLICATIONS_LINK_KEY)));

        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure the post API can handle bad input.
     *
     * @throws Exception on issue
     */
    @Test
    public void canHandleBadInputToCreateCommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        final Command cluster = new Command.Builder(null, null, null, null, null, -1L).build();
        this.mvc.perform(
            MockMvcRequestBuilders
                .post(COMMANDS_API)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsBytes(cluster))
        ).andExpect(MockMvcResultMatchers.status().isPreconditionFailed());
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can search for commands by various parameters.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canFindCommands() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        final String id1 = UUID.randomUUID().toString();
        final String id2 = UUID.randomUUID().toString();
        final String id3 = UUID.randomUUID().toString();
        final String name1 = UUID.randomUUID().toString();
        final String name2 = UUID.randomUUID().toString();
        final String name3 = UUID.randomUUID().toString();
        final String user1 = UUID.randomUUID().toString();
        final String user2 = UUID.randomUUID().toString();
        final String user3 = UUID.randomUUID().toString();
        final String version1 = UUID.randomUUID().toString();
        final String version2 = UUID.randomUUID().toString();
        final String version3 = UUID.randomUUID().toString();
        final String executable1 = UUID.randomUUID().toString();
        final String executable2 = UUID.randomUUID().toString();
        final String executable3 = UUID.randomUUID().toString();

        this.createConfigResource(
            new Command
                .Builder(name1, user1, version1, CommandStatus.ACTIVE, executable1, CHECK_DELAY)
                .withId(id1)
                .build(),
            null
        );
        Thread.sleep(1000);
        this.createConfigResource(
            new Command
                .Builder(name2, user2, version2, CommandStatus.DEPRECATED, executable2, CHECK_DELAY)
                .withId(id2)
                .build(),
            null
        );
        Thread.sleep(1000);
        this.createConfigResource(
            new Command
                .Builder(name3, user3, version3, CommandStatus.INACTIVE, executable3, CHECK_DELAY)
                .withId(id3)
                .build(),
            null
        );

        // Test finding all commands
        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH, Matchers.hasSize(3)));

        // Try to limit the number of results
        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API).param("size", "2"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH, Matchers.hasSize(2)));

        // Query by name
        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API).param("name", name2))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH + "[0].id", Matchers.is(id2)));

        // Query by user
        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API).param("user", user3))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH + "[0].id", Matchers.is(id3)));

        // Query by statuses
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .get(COMMANDS_API)
                    .param("status", CommandStatus.ACTIVE.toString(), CommandStatus.INACTIVE.toString())
            )
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH, Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH + "[0].id", Matchers.is(id3)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH + "[1].id", Matchers.is(id1)));

        // Query by tags
        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API).param("tag", "genie.id:" + id1))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMANDS_LIST_PATH + "[0].id", Matchers.is(id1)));

        //TODO: Add tests for sort, orderBy etc

        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(3L));
    }

    /**
     * Test to make sure that a command can be updated.
     *
     * @throws Exception on configuration errors
     */
    @Test
    public void canUpdateCommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String commandResource = COMMANDS_API + "/" + ID;
        final Command createdCommand = objectMapper
            .readValue(
                this.mvc.perform(
                    MockMvcRequestBuilders.get(commandResource)
                )
                    .andReturn()
                    .getResponse()
                    .getContentAsByteArray(),
                CommandResource.class
            ).getContent();
        Assert.assertThat(createdCommand.getStatus(), Matchers.is(CommandStatus.ACTIVE));

        final Command.Builder updateCommand = new Command.Builder(
            createdCommand.getName(),
            createdCommand.getUser(),
            createdCommand.getVersion(),
            CommandStatus.INACTIVE,
            createdCommand.getExecutable(),
            createdCommand.getCheckDelay()
        )
            .withId(createdCommand.getId().orElseThrow(IllegalArgumentException::new))
            .withCreated(createdCommand.getCreated().orElseThrow(IllegalArgumentException::new))
            .withUpdated(createdCommand.getUpdated().orElseThrow(IllegalArgumentException::new))
            .withTags(createdCommand.getTags())
            .withConfigs(createdCommand.getConfigs());

        createdCommand.getDescription().ifPresent(updateCommand::withDescription);
        createdCommand.getSetupFile().ifPresent(updateCommand::withSetupFile);

        this.mvc.perform(
            MockMvcRequestBuilders
                .put(commandResource)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsBytes(updateCommand.build()))
        ).andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(commandResource))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(
                MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(CommandStatus.INACTIVE.toString()))
            );
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure that a command can be patched.
     *
     * @throws Exception on configuration errors
     */
    @Test
    public void canPatchCommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        final String id = this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String commandResource = COMMANDS_API + "/" + id;
        this.mvc
            .perform(MockMvcRequestBuilders.get(commandResource))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(NAME)));


        final String newName = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + newName + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(objectMapper.readTree(patchString));

        this.mvc.perform(
            MockMvcRequestBuilders
                .patch(commandResource)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsBytes(patch))
        ).andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(commandResource))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(newName)));
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(1L));
    }

    /**
     * Make sure can successfully delete all commands.
     *
     * @throws Exception on a configuration error
     */
    @Test
    public void canDeleteAllCommands() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.DEPRECATED, EXECUTABLE, CHECK_DELAY)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.INACTIVE, EXECUTABLE, CHECK_DELAY)
                .build(),
            null
        );
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(3L));

        this.mvc
            .perform(MockMvcRequestBuilders.delete(COMMANDS_API))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can delete a command.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canDeleteACommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        final String id1 = UUID.randomUUID().toString();
        final String id2 = UUID.randomUUID().toString();
        final String id3 = UUID.randomUUID().toString();
        final String name1 = UUID.randomUUID().toString();
        final String name2 = UUID.randomUUID().toString();
        final String name3 = UUID.randomUUID().toString();
        final String user1 = UUID.randomUUID().toString();
        final String user2 = UUID.randomUUID().toString();
        final String user3 = UUID.randomUUID().toString();
        final String version1 = UUID.randomUUID().toString();
        final String version2 = UUID.randomUUID().toString();
        final String version3 = UUID.randomUUID().toString();
        final String executable1 = UUID.randomUUID().toString();
        final String executable2 = UUID.randomUUID().toString();
        final String executable3 = UUID.randomUUID().toString();

        this.createConfigResource(
            new Command
                .Builder(name1, user1, version1, CommandStatus.ACTIVE, executable1, CHECK_DELAY)
                .withId(id1)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(name2, user2, version2, CommandStatus.ACTIVE, executable2, CHECK_DELAY)
                .withId(id2)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(name3, user3, version3, CommandStatus.ACTIVE, executable3, CHECK_DELAY)
                .withId(id3)
                .build(),
            null
        );
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(3L));

        this.mvc
            .perform(MockMvcRequestBuilders.delete(COMMANDS_API + "/" + id2))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API + "/" + id2))
            .andExpect(MockMvcResultMatchers.status().isNotFound());

        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(2L));
    }

    /**
     * Test to make sure we can add configurations to the command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddConfigsToCommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        this.canAddElementsToResource(COMMANDS_API + "/" + ID + "/configs");
    }

    /**
     * Test to make sure we can update the configurations for a command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateConfigsForCommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        this.canUpdateElementsForResource(COMMANDS_API + "/" + ID + "/configs");
    }

    /**
     * Test to make sure we can delete the configurations for a command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteConfigsForCommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        this.canDeleteElementsFromResource(COMMANDS_API + "/" + ID + "/configs");
    }

    /**
     * Test to make sure we can add tags to the command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddTagsToCommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String api = COMMANDS_API + "/" + ID + "/tags";
        this.canAddTagsToResource(api, ID, NAME);
    }

    /**
     * Test to make sure we can update the tags for a command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateTagsForCommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String api = COMMANDS_API + "/" + ID + "/tags";
        this.canUpdateTagsForResource(api, ID, NAME);
    }

    /**
     * Test to make sure we can delete the tags for a command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteTagsForCommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String api = COMMANDS_API + "/" + ID + "/tags";
        this.canDeleteTagsForResource(api, ID, NAME);
    }

    /**
     * Test to make sure we can delete a tag for a command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteTagForCommand() throws Exception {
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String api = COMMANDS_API + "/" + ID + "/tags";
        this.canDeleteTagForResource(api, ID, NAME);
    }

    /**
     * Make sure can add the applications for a command.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canAddApplicationsForACommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String commandApplicationsAPI = COMMANDS_API + "/" + ID + "/applications";
        this.mvc
            .perform(MockMvcRequestBuilders.get(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));

        final String placeholder = UUID.randomUUID().toString();
        final String applicationId1 = UUID.randomUUID().toString();
        final String applicationId2 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId1)
                .build(),
            null
        );
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId2)
                .build(),
            null
        );

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(commandApplicationsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(applicationId1, applicationId2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(applicationId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(applicationId2)));

        //Shouldn't add anything
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(commandApplicationsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList()))
            )
            .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());

        final String applicationId3 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId3)
                .build(),
            null
        );
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(commandApplicationsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(applicationId3)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(applicationId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(applicationId2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[2].id", Matchers.is(applicationId3)));
    }

    /**
     * Make sure can set the applications for a command.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canSetApplicationsForACommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String commandApplicationsAPI = COMMANDS_API + "/" + ID + "/applications";
        this.mvc
            .perform(MockMvcRequestBuilders.get(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));

        final String placeholder = UUID.randomUUID().toString();
        final String applicationId1 = UUID.randomUUID().toString();
        final String applicationId2 = UUID.randomUUID().toString();
        final String applicationId3 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId1)
                .build(),
            null
        );
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId2)
                .build(),
            null
        );
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId3)
                .build(),
            null
        );

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .put(commandApplicationsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(applicationId1, applicationId2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(applicationId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(applicationId2)));

        // Should flip the order
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .put(commandApplicationsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(applicationId2, applicationId1)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(applicationId2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(applicationId1)));

        // Should reorder and add a new one
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .put(commandApplicationsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        objectMapper.writeValueAsBytes(
                            Lists.newArrayList(applicationId1, applicationId2, applicationId3))
                    )
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(applicationId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(applicationId2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[2].id", Matchers.is(applicationId3)));

        //Should clear applications
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .put(commandApplicationsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList()))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));
    }

    /**
     * Make sure that we can remove all the applications from a command.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canRemoveApplicationsFromACommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String commandApplicationsAPI = COMMANDS_API + "/" + ID + "/applications";

        final String placeholder = UUID.randomUUID().toString();
        final String applicationId1 = UUID.randomUUID().toString();
        final String applicationId2 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId1)
                .build(),
            null
        );
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId2)
                .build(),
            null
        );

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(commandApplicationsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(applicationId1, applicationId2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.delete(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));
    }

    /**
     * Make sure that we can remove an application from a command.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canRemoveApplicationFromACommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String commandApplicationsAPI = COMMANDS_API + "/" + ID + "/applications";

        final String placeholder = UUID.randomUUID().toString();
        final String applicationId1 = UUID.randomUUID().toString();
        final String applicationId2 = UUID.randomUUID().toString();
        final String applicationId3 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId1)
                .build(),
            null
        );
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId2)
                .build(),
            null
        );
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId3)
                .build(),
            null
        );

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(commandApplicationsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        objectMapper.writeValueAsBytes(
                            Lists.newArrayList(applicationId1, applicationId2, applicationId3)
                        )
                    )
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.delete(commandApplicationsAPI + "/" + applicationId2))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(commandApplicationsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(applicationId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(applicationId3)));

        // Check reverse side of relationship
        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API + "/" + applicationId1 + "/commands"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(ID)));

        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API + "/" + applicationId2 + "/commands"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));

        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API + "/" + applicationId3 + "/commands"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(ID)));
    }

    /**
     * Make sure can get all the clusters which use a given command.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canGetClustersForCommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String placeholder = UUID.randomUUID().toString();
        final String cluster1Id = UUID.randomUUID().toString();
        final String cluster2Id = UUID.randomUUID().toString();
        final String cluster3Id = UUID.randomUUID().toString();
        this.createConfigResource(
            new Cluster.Builder(placeholder, placeholder, placeholder, ClusterStatus.UP).withId(cluster1Id).build(),
            null
        );
        this.createConfigResource(
            new Cluster
                .Builder(placeholder, placeholder, placeholder, ClusterStatus.OUT_OF_SERVICE)
                .withId(cluster2Id)
                .build(),
            null
        );
        this.createConfigResource(
            new Cluster
                .Builder(placeholder, placeholder, placeholder, ClusterStatus.TERMINATED)
                .withId(cluster3Id)
                .build(),
            null
        );

        final List<String> commandIds = Lists.newArrayList(ID);
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(CLUSTERS_API + "/" + cluster1Id + "/commands")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(commandIds))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(CLUSTERS_API + "/" + cluster3Id + "/commands")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(commandIds))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        Arrays.stream(
            objectMapper.readValue(
                this.mvc
                    .perform(MockMvcRequestBuilders.get(COMMANDS_API + "/" + ID + "/clusters"))
                    .andExpect(MockMvcResultMatchers.status().isOk())
                    .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
                    .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
                    .andReturn()
                    .getResponse()
                    .getContentAsByteArray(), ClusterResource[].class
            )
        )
            .map(ClusterResource::getContent)
            .forEach(
                cluster -> {
                    final String id = cluster.getId().orElseThrow(IllegalArgumentException::new);
                    if (!id.equals(cluster1Id) && !id.equals(cluster3Id)) {
                        Assert.fail();
                    }
                }
            );

        // Test filtering
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .get(COMMANDS_API + "/" + ID + "/clusters")
                    .param("status", ClusterStatus.UP.toString())
            )
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(cluster1Id)));
    }
}
