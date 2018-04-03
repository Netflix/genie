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
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import com.netflix.genie.web.hateoas.resources.CommandResource;
import io.restassured.RestAssured;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.restdocs.payload.PayloadDocumentation;
import org.springframework.restdocs.request.RequestDocumentation;
import org.springframework.restdocs.restassured3.RestAssuredRestDocumentation;
import org.springframework.restdocs.restassured3.RestDocumentationFilter;
import org.springframework.restdocs.snippet.Attributes;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
    private static final String DESCRIPTION = "Hive command v" + VERSION;
    private static final int MEMORY = 1024;
    private static final String CONFIG_1 = "s3:///path/to/config-foo";
    private static final String CONFIG_2 = "s3:///path/to/config-bar";
    private static final Set<String> CONFIGS = Sets.newHashSet(CONFIG_1, CONFIG_2);
    private static final String DEP_1 = "/path/to/file/foo";
    private static final String DEP_2 = "/path/to/file/bar";
    private static final Set<String> DEPENDENCIES = Sets.newHashSet(DEP_1, DEP_2);
    private static final String TAG_1 = "tag:foo";
    private static final String TAG_2 = "tag:bar";
    private static final Set<String> TAGS = Sets.newHashSet(TAG_1, TAG_2);

    private static final String EXECUTABLE_PATH = "executable";
    private static final String CHECK_DELAY_PATH = "checkDelay";
    private static final String MEMORY_PATH = "memory";
    private static final String COMMANDS_LIST_PATH = EMBEDDED_PATH + ".commandList";
    private static final String COMMAND_APPS_LINK_PATH = "_links.applications.href";
    private static final String COMMANDS_APPS_LINK_PATH = COMMANDS_LIST_PATH + "._links.applications.href";
    private static final String COMMAND_CLUSTERS_LINK_PATH = "_links.clusters.href";
    private static final String COMMANDS_CLUSTERS_LINK_PATH = COMMANDS_LIST_PATH + "._links.clusters.href";
    private static final String COMMANDS_ID_LIST_PATH = EMBEDDED_PATH + ".commandList.id";

    /**
     * {@inheritDoc}
     */
    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
    }

    /**
     * {@inheritDoc}
     */
    @After
    @Override
    public void cleanup() throws Exception {
        super.cleanup();
    }

    /**
     * Test creating a command without an ID.
     *
     * @throws Exception on configuration issue
     */
    @Test
    public void canCreateCommandWithoutId() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));

        final RestDocumentationFilter createFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request headers
            Snippets.getCommandRequestPayload(), // Request fields
            Snippets.LOCATION_HEADER // Response headers
        );

        final String id = this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withDescription(DESCRIPTION)
                .withMemory(MEMORY)
                .withConfigs(CONFIGS)
                .withDependencies(DEPENDENCIES)
                .withTags(TAGS)
                .build(),
            createFilter
        );

        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // response headers
            Snippets.getCommandResponsePayload(), // response payload
            Snippets.COMMAND_LINKS // response links
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getFilter)
            .when()
            .port(this.port)
            .get(COMMANDS_API + "/{id}", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(NAME))
            .body(USER_PATH, Matchers.is(USER))
            .body(VERSION_PATH, Matchers.is(VERSION))
            .body(STATUS_PATH, Matchers.is(CommandStatus.ACTIVE.toString()))
            .body(EXECUTABLE_PATH, Matchers.is(EXECUTABLE))
            .body(CHECK_DELAY_PATH, Matchers.is((int) CHECK_DELAY))
            .body(DESCRIPTION_PATH, Matchers.is(DESCRIPTION))
            .body(MEMORY_PATH, Matchers.is(MEMORY))
            .body(CONFIGS_PATH, Matchers.hasSize(2))
            .body(CONFIGS_PATH, Matchers.hasItems(CONFIG_1, CONFIG_2))
            .body(DEPENDENCIES_PATH, Matchers.hasSize(2))
            .body(DEPENDENCIES_PATH, Matchers.hasItems(DEP_1, DEP_2))
            .body(TAGS_PATH, Matchers.hasSize(4))
            .body(TAGS_PATH, Matchers.hasItem("genie.id:" + id))
            .body(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME))
            .body(TAGS_PATH, Matchers.hasItems(TAG_1, TAG_2))
            .body(LINKS_PATH + ".keySet().size()", Matchers.is(3))
            .body(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(CLUSTERS_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(APPLICATIONS_LINK_KEY))
            .body(
                COMMAND_CLUSTERS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    COMMANDS_API,
                    CLUSTERS_LINK_KEY,
                    CLUSTERS_OPTIONAL_HAL_LINK_PARAMETERS,
                    id
                )
            )
            .body(
                COMMAND_APPS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    COMMANDS_API,
                    APPLICATIONS_LINK_KEY,
                    null,
                    id
                )
            );

        Assert.assertThat(this.commandRepository.count(), Matchers.is(1L));
    }

    /**
     * Test creating a Command with an ID.
     *
     * @throws Exception When issue in creation
     */
    @Test
    public void canCreateCommandWithId() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .withDescription(DESCRIPTION)
                .withMemory(MEMORY)
                .withConfigs(CONFIGS)
                .withDependencies(DEPENDENCIES)
                .withTags(TAGS)
                .build(),
            null
        );

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(COMMANDS_API + "/{id}", ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(ID_PATH, Matchers.is(ID))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(NAME))
            .body(USER_PATH, Matchers.is(USER))
            .body(VERSION_PATH, Matchers.is(VERSION))
            .body(STATUS_PATH, Matchers.is(CommandStatus.ACTIVE.toString()))
            .body(EXECUTABLE_PATH, Matchers.is(EXECUTABLE))
            .body(CHECK_DELAY_PATH, Matchers.is((int) CHECK_DELAY))
            .body(DESCRIPTION_PATH, Matchers.is(DESCRIPTION))
            .body(MEMORY_PATH, Matchers.is(MEMORY))
            .body(CONFIGS_PATH, Matchers.hasSize(2))
            .body(CONFIGS_PATH, Matchers.hasItems(CONFIG_1, CONFIG_2))
            .body(DEPENDENCIES_PATH, Matchers.hasSize(2))
            .body(DEPENDENCIES_PATH, Matchers.hasItems(DEP_1, DEP_2))
            .body(TAGS_PATH, Matchers.hasSize(4))
            .body(TAGS_PATH, Matchers.hasItem("genie.id:" + ID))
            .body(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME))
            .body(TAGS_PATH, Matchers.hasItems(TAG_1, TAG_2))
            .body(LINKS_PATH + ".keySet().size()", Matchers.is(3))
            .body(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(CLUSTERS_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(APPLICATIONS_LINK_KEY))
            .body(
                COMMAND_CLUSTERS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    COMMANDS_API,
                    CLUSTERS_LINK_KEY,
                    CLUSTERS_OPTIONAL_HAL_LINK_PARAMETERS,
                    ID
                )
            )
            .body(
                COMMAND_APPS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    COMMANDS_API,
                    APPLICATIONS_LINK_KEY,
                    null,
                    ID
                )
            );

        Assert.assertThat(this.commandRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure the post API can handle bad input.
     *
     * @throws Exception on issue
     */
    @Test
    public void canHandleBadInputToCreateCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        final Command cluster = new Command.Builder(" ", " ", " ", CommandStatus.ACTIVE, " ", -1L).build();
        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(cluster))
            .when()
            .port(this.port)
            .post(COMMANDS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()));

        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can search for commands by various parameters.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canFindCommands() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
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

        final RestDocumentationFilter findFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.COMMAND_SEARCH_QUERY_PARAMETERS, // Request query parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response headers
            Snippets.COMMAND_SEARCH_RESULT_FIELDS, // Result fields
            Snippets.SEARCH_LINKS // HAL Links
        );

        // Test finding all commands
        RestAssured
            .given(this.getRequestSpecification())
            .filter(findFilter)
            .when()
            .port(this.port)
            .get(COMMANDS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(COMMANDS_LIST_PATH, Matchers.hasSize(3))
            .body(COMMANDS_ID_LIST_PATH, Matchers.containsInAnyOrder(id1, id2, id3))
            .body(
                COMMANDS_APPS_LINK_PATH,
                EntitiesLinksMatcher
                    .matchUrisAnyOrder(
                        COMMANDS_API,
                        APPLICATIONS_LINK_KEY,
                        null,
                        Lists.newArrayList(id1, id2, id3)
                    )
            )
            .body(
                COMMANDS_CLUSTERS_LINK_PATH,
                EntitiesLinksMatcher.
                    matchUrisAnyOrder(
                        COMMANDS_API,
                        CLUSTERS_LINK_KEY,
                        CLUSTERS_OPTIONAL_HAL_LINK_PARAMETERS,
                        Lists.newArrayList(id1, id2, id3)
                    )
            );

        // Try to limit the number of results
        RestAssured
            .given(this.getRequestSpecification())
            .filter(findFilter)
            .param("size", 2)
            .when()
            .port(this.port)
            .get(COMMANDS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(COMMANDS_LIST_PATH, Matchers.hasSize(2));

        // Query by name
        RestAssured
            .given(this.getRequestSpecification())
            .filter(findFilter)
            .param("name", name2)
            .when()
            .port(this.port)
            .get(COMMANDS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(COMMANDS_LIST_PATH, Matchers.hasSize(1))
            .body(COMMANDS_LIST_PATH + "[0].id", Matchers.is(id2));

        // Query by user
        RestAssured
            .given(this.getRequestSpecification())
            .filter(findFilter)
            .param("user", user3)
            .when()
            .port(this.port)
            .get(COMMANDS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(COMMANDS_LIST_PATH, Matchers.hasSize(1))
            .body(COMMANDS_LIST_PATH + "[0].id", Matchers.is(id3));

        // Query by statuses
        RestAssured
            .given(this.getRequestSpecification())
            .filter(findFilter)
            .param("status", CommandStatus.ACTIVE.toString(), CommandStatus.INACTIVE.toString())
            .when()
            .port(this.port)
            .get(COMMANDS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(COMMANDS_LIST_PATH, Matchers.hasSize(2))
            .body(COMMANDS_LIST_PATH + "[0].id", Matchers.is(id3))
            .body(COMMANDS_LIST_PATH + "[1].id", Matchers.is(id1));

        // Query by tags
        RestAssured
            .given(this.getRequestSpecification())
            .filter(findFilter)
            .param("tag", "genie.id:" + id1)
            .when()
            .port(this.port)
            .get(COMMANDS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(COMMANDS_LIST_PATH, Matchers.hasSize(1))
            .body(COMMANDS_LIST_PATH + "[0].id", Matchers.is(id1));

        //TODO: Add tests for sort, orderBy etc

        Assert.assertThat(this.commandRepository.count(), Matchers.is(3L));
    }

    /**
     * Test to make sure that a command can be updated.
     *
     * @throws Exception on configuration errors
     */
    @Test
    public void canUpdateCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String commandResource = COMMANDS_API + "/{id}";
        final Command createdCommand = GenieObjectMapper.getMapper()
            .readValue(
                RestAssured
                    .given(this.getRequestSpecification())
                    .when()
                    .port(this.port)
                    .get(commandResource, ID)
                    .then()
                    .statusCode(Matchers.is(HttpStatus.OK.value()))
                    .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
                    .extract()
                    .asByteArray(),
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
            .withConfigs(createdCommand.getConfigs())
            .withDependencies(createdCommand.getDependencies());

        createdCommand.getDescription().ifPresent(updateCommand::withDescription);
        createdCommand.getSetupFile().ifPresent(updateCommand::withSetupFile);

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // request header
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.getCommandRequestPayload() // payload fields
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(updateFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(updateCommand.build()))
            .when()
            .port(this.port)
            .put(commandResource, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandResource, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(STATUS_PATH, Matchers.is(CommandStatus.INACTIVE.toString()));

        Assert.assertThat(this.commandRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure that a command can be patched.
     *
     * @throws Exception on configuration errors
     */
    @Test
    public void canPatchCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        final String id = this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String commandResource = COMMANDS_API + "/{id}";
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandResource, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(NAME_PATH, Matchers.is(NAME));


        final String newName = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + newName + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));

        final RestDocumentationFilter patchFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // request headers
            Snippets.ID_PATH_PARAM, // path params
            Snippets.PATCH_FIELDS // request payload
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(patchFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(patch))
            .when()
            .port(this.port)
            .patch(commandResource, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandResource, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(NAME_PATH, Matchers.is(newName));

        Assert.assertThat(this.commandRepository.count(), Matchers.is(1L));
    }

    /**
     * Make sure can successfully delete all commands.
     *
     * @throws Exception on a configuration error
     */
    @Test
    public void canDeleteAllCommands() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
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
        Assert.assertThat(this.commandRepository.count(), Matchers.is(3L));

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/"
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(COMMANDS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can delete a command.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canDeleteACommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
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
        Assert.assertThat(this.commandRepository.count(), Matchers.is(3L));

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM // path parameters
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(COMMANDS_API + "/{id}", id2)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(COMMANDS_API + "/{id}", id2)
            .then()
            .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()));

        Assert.assertThat(this.commandRepository.count(), Matchers.is(2L));
    }

    /**
     * Test to make sure we can add configurations to the command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddConfigsToCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );

        final RestDocumentationFilter addFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // path params
            Snippets.CONTENT_TYPE_HEADER, // request header
            PayloadDocumentation.requestFields(Snippets.CONFIG_FIELDS) // response fields
        );
        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // path params
            Snippets.JSON_CONTENT_TYPE_HEADER, // response headers
            PayloadDocumentation.responseFields(Snippets.CONFIG_FIELDS) // response fields
        );
        this.canAddElementsToResource(COMMANDS_API + "/{id}/configs", ID, addFilter, getFilter);
    }

    /**
     * Test to make sure we can update the configurations for a command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateConfigsForCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.CONFIG_FIELDS) // Request fields
        );
        this.canUpdateElementsForResource(COMMANDS_API + "/{id}/configs", ID, updateFilter);
    }

    /**
     * Test to make sure we can delete the configurations for a command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteConfigsForCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM
        );
        this.canDeleteElementsFromResource(COMMANDS_API + "/{id}/configs", ID, deleteFilter);
    }

    /**
     * Test to make sure we can add dependencies to the command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddDependenciesToCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );

        final RestDocumentationFilter addFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // path params
            Snippets.CONTENT_TYPE_HEADER, // request header
            PayloadDocumentation.requestFields(Snippets.DEPENDENCIES_FIELDS) // response fields
        );
        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // path params
            Snippets.JSON_CONTENT_TYPE_HEADER, // response headers
            PayloadDocumentation.responseFields(Snippets.DEPENDENCIES_FIELDS) // response fields
        );
        this.canAddElementsToResource(
            COMMANDS_API + "/{id}/dependencies",
            ID,
            addFilter,
            getFilter
        );
    }

    /**
     * Test to make sure we can update the dependencies for an command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateDependenciesForCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.DEPENDENCIES_FIELDS) // Request fields
        );
        this.canUpdateElementsForResource(
            COMMANDS_API + "/{id}/dependencies",
            ID,
            updateFilter
        );
    }

    /**
     * Test to make sure we can delete the dependencies for an command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteDependenciesForCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM // Path variables
        );
        this.canDeleteElementsFromResource(
            COMMANDS_API + "/{id}/dependencies",
            ID,
            deleteFilter
        );
    }

    /**
     * Test to make sure we can add tags to the command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddTagsToCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String api = COMMANDS_API + "/{id}/tags";

        final RestDocumentationFilter addFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.TAGS_FIELDS) // Request fields
        );
        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.JSON_CONTENT_TYPE_HEADER, // Response header
            PayloadDocumentation.responseFields(Snippets.TAGS_FIELDS)
        );
        this.canAddTagsToResource(api, ID, NAME, addFilter, getFilter);
    }

    /**
     * Test to make sure we can update the tags for a command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateTagsForCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String api = COMMANDS_API + "/{id}/tags";

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.TAGS_FIELDS) // Request fields
        );
        this.canUpdateTagsForResource(api, ID, NAME, updateFilter);
    }

    /**
     * Test to make sure we can delete the tags for a command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteTagsForCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String api = COMMANDS_API + "/{id}/tags";

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM
        );
        this.canDeleteTagsForResource(api, ID, NAME, deleteFilter);
    }

    /**
     * Test to make sure we can delete a tag for a command after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteTagForCommand() throws Exception {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(ID)
                .build(),
            null
        );
        final String api = COMMANDS_API + "/{id}/tags";

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM.and(
                RequestDocumentation.parameterWithName("tag").description("The tag to remove")
            )
        );
        this.canDeleteTagForResource(api, ID, NAME, deleteFilter);
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
        final String commandApplicationsAPI = COMMANDS_API + "/{id}/applications";

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.empty());

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

        final RestDocumentationFilter addFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request Headers
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(
                PayloadDocumentation
                    .fieldWithPath("[]")
                    .description("Array of application ids to add to existing set of applications")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            ) // Request payload
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(addFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(
                GenieObjectMapper
                    .getMapper()
                    .writeValueAsBytes(Lists.newArrayList(applicationId1, applicationId2))
            )
            .when()
            .port(this.port)
            .post(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(2))
            .body("[0].id", Matchers.is(applicationId1))
            .body("[1].id", Matchers.is(applicationId2));

        //Shouldn't add anything
        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Lists.newArrayList()))
            .when()
            .port(this.port)
            .post(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()));

        final String applicationId3 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Application
                .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                .withId(applicationId3)
                .build(),
            null
        );
        RestAssured
            .given(this.getRequestSpecification())
            .filter(addFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Lists.newArrayList(applicationId3)))
            .when()
            .port(this.port)
            .post(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            PayloadDocumentation.responseFields(
                PayloadDocumentation
                    .subsectionWithPath("[]")
                    .description("The set of applications this command depends on")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            )
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getFilter)
            .when()
            .port(this.port)
            .get(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(3))
            .body("[0].id", Matchers.is(applicationId1))
            .body("[1].id", Matchers.is(applicationId2))
            .body("[2].id", Matchers.is(applicationId3));
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
        final String commandApplicationsAPI = COMMANDS_API + "/{id}/applications";
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.empty());

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

        final RestDocumentationFilter setFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request Headers
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(
                PayloadDocumentation
                    .fieldWithPath("[]")
                    .description("Array of application ids to replace the existing set of applications with")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            ) // Request payload
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(setFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Lists.newArrayList(applicationId1, applicationId2)))
            .when()
            .port(this.port)
            .put(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(2))
            .body("[0].id", Matchers.is(applicationId1))
            .body("[1].id", Matchers.is(applicationId2));

        // Should flip the order
        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Lists.newArrayList(applicationId2, applicationId1)))
            .when()
            .port(this.port)
            .put(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(2))
            .body("[0].id", Matchers.is(applicationId2))
            .body("[1].id", Matchers.is(applicationId1));

        // Should reorder and add a new one
        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(
                GenieObjectMapper
                    .getMapper()
                    .writeValueAsBytes(Lists.newArrayList(applicationId1, applicationId2, applicationId3))
            )
            .when()
            .port(this.port)
            .put(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(3))
            .body("[0].id", Matchers.is(applicationId1))
            .body("[1].id", Matchers.is(applicationId2))
            .body("[2].id", Matchers.is(applicationId3));

        //Should clear applications
        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Lists.newArrayList()))
            .when()
            .port(this.port)
            .put(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.empty());
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
        final String commandApplicationsAPI = COMMANDS_API + "/{id}/applications";

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

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Lists.newArrayList(applicationId1, applicationId2)))
            .when()
            .port(this.port)
            .post(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM // Path parameters
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.empty());
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
        final String commandApplicationsAPI = COMMANDS_API + "/{id}/applications";

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

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(
                GenieObjectMapper
                    .getMapper()
                    .writeValueAsBytes(Lists.newArrayList(applicationId1, applicationId2, applicationId3))
            )
            .when()
            .port(this.port)
            .post(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM.and(
                RequestDocumentation
                    .parameterWithName("applicationId")
                    .description("The id of the application to remove")
            ) // Path parameters
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(commandApplicationsAPI + "/{applicationId}", ID, applicationId2)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(commandApplicationsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(2))
            .body("[0].id", Matchers.is(applicationId1))
            .body("[1].id", Matchers.is(applicationId3));

        // Check reverse side of relationship
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(APPLICATIONS_API + "/{id}/commands", applicationId1)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(1))
            .body("[0].id", Matchers.is(ID));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(APPLICATIONS_API + "/{id}/commands", applicationId2)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.empty());

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(APPLICATIONS_API + "/{id}/commands", applicationId3)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(1))
            .body("[0].id", Matchers.is(ID));
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
        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(commandIds))
            .when()
            .port(this.port)
            .post(CLUSTERS_API + "/{id}/commands", cluster1Id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(commandIds))
            .when()
            .port(this.port)
            .post(CLUSTERS_API + "/{id}/commands", cluster3Id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        Arrays.stream(
            GenieObjectMapper.getMapper().readValue(
                RestAssured
                    .given(this.getRequestSpecification())
                    .when()
                    .port(this.port)
                    .get(COMMANDS_API + "/{id}/clusters", ID)
                    .then()
                    .statusCode(Matchers.is(HttpStatus.OK.value()))
                    .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
                    .body("$", Matchers.hasSize(2))
                    .extract()
                    .asByteArray(),
                ClusterResource[].class
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
        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // Path parameters
            RequestDocumentation.requestParameters(
                RequestDocumentation
                    .parameterWithName("status")
                    .description("The status of clusters to search for")
                    .attributes(
                        Attributes.key(Snippets.CONSTRAINTS).value(CommandStatus.values())
                    )
                    .optional()
            ), // Query Parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            PayloadDocumentation.responseFields(
                PayloadDocumentation
                    .subsectionWithPath("[]")
                    .description("The list of clusters found")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            )
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getFilter)
            .param("status", ClusterStatus.UP.toString())
            .when()
            .port(this.port)
            .get(COMMANDS_API + "/{id}/clusters", ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(1))
            .body("[0].id", Matchers.is(cluster1Id));
    }

    /**
     * Test creating a command with blank files and tag resources.
     *
     * @throws Exception when an unexpected error is encountered
     */
    @Test
    public void canCreateCommandWithBlankFields() throws Exception {

        final Set<String> stringSetWithBlank = Sets.newHashSet("foo", " ");

        final List<Command> invalidCommandResources = Lists.newArrayList(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(UUID.randomUUID().toString())
                .withSetupFile(" ")
                .build(),

            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(UUID.randomUUID().toString())
                .withConfigs(stringSetWithBlank)
                .build(),

            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(UUID.randomUUID().toString())
                .withDependencies(stringSetWithBlank)

                .build(),

            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
                .withId(UUID.randomUUID().toString())
                .withTags(stringSetWithBlank)
                .build()
        );

        long i = 0L;
        for (final Command invalidCommandResource : invalidCommandResources) {
            Assert.assertThat(this.commandRepository.count(), Matchers.is(i));

            RestAssured
                .given(this.getRequestSpecification())
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(GenieObjectMapper.getMapper().writeValueAsBytes(invalidCommandResource))
                .when()
                .port(this.port)
                .post(COMMANDS_API)
                .then()
                .statusCode(Matchers.is(HttpStatus.CREATED.value()));

            Assert.assertThat(this.commandRepository.count(), Matchers.is(++i));
        }
    }
}
