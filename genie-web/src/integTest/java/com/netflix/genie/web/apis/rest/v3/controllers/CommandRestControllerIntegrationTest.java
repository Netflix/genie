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
package com.netflix.genie.web.apis.rest.v3.controllers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ResolvedResources;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.hateoas.EntityModel;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Integration tests for the Commands REST API.
 *
 * @author tgianos
 * @since 3.0.0
 */
class CommandRestControllerIntegrationTest extends RestControllerIntegrationTestBase {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = "hive";
    private static final String USER = "genie";
    private static final String VERSION = "1.0.0";
    private static final String EXECUTABLE = "/apps/hive/bin/hive";
    private static final ImmutableList<String> EXECUTABLE_AND_ARGS = ImmutableList.of("/apps/hive/bin/hive");
    private static final long CHECK_DELAY = 10000L;
    private static final String DESCRIPTION = "Hive command v" + VERSION;
    private static final int MEMORY = 1024;
    private static final String CONFIG_1 = "s3:///path/to/config-foo";
    private static final String CONFIG_2 = "s3:///path/to/config-bar";
    private static final ImmutableSet<String> CONFIGS = ImmutableSet.of(CONFIG_1, CONFIG_2);
    private static final String DEP_1 = "/path/to/file/foo";
    private static final String DEP_2 = "/path/to/file/bar";
    private static final ImmutableSet<String> DEPENDENCIES = ImmutableSet.of(DEP_1, DEP_2);
    private static final String TAG_1 = "tag:foo";
    private static final String TAG_2 = "tag:bar";
    private static final Set<String> TAGS = Sets.newHashSet(TAG_1, TAG_2);
    private static final ImmutableList<Criterion> CLUSTER_CRITERIA = ImmutableList.of(
        new Criterion
            .Builder()
            .withId(UUID.randomUUID().toString())
            .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            .build(),
        new Criterion
            .Builder()
            .withId(UUID.randomUUID().toString())
            .withName("prod")
            .withVersion("1.0.0")
            .withStatus(ClusterStatus.UP.name())
            .withTags(
                Sets.newHashSet(
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString()
                )
            )
            .build()
    );

    private static final String EXECUTABLE_PATH = "executable";
    private static final String EXECUTABLE_AND_ARGS_PATH = "executableAndArguments";
    private static final String CHECK_DELAY_PATH = "checkDelay";
    private static final String MEMORY_PATH = "memory";
    private static final String CLUSTER_CRITERIA_PATH = "clusterCriteria";
    private static final String COMMANDS_LIST_PATH = EMBEDDED_PATH + ".commandList";
    private static final String COMMAND_APPS_LINK_PATH = "_links.applications.href";
    private static final String COMMANDS_APPS_LINK_PATH = COMMANDS_LIST_PATH + "._links.applications.href";
    private static final String COMMAND_CLUSTERS_LINK_PATH = "_links.clusters.href";
    private static final String COMMANDS_CLUSTERS_LINK_PATH = COMMANDS_LIST_PATH + "._links.clusters.href";
    private static final String COMMANDS_ID_LIST_PATH = EMBEDDED_PATH + ".commandList.id";

    @BeforeEach
    void beforeCommands() {
        Assertions.assertThat(this.commandRepository.count()).isEqualTo(0L);
    }

    @Test
    void canCreateCommandWithoutId() throws Exception {
        final RestDocumentationFilter createFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request headers
            Snippets.getCommandRequestPayload(), // Request fields
            Snippets.LOCATION_HEADER // Response headers
        );

        final String id = this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withDescription(DESCRIPTION)
                .withMemory(MEMORY)
                .withConfigs(CONFIGS)
                .withDependencies(DEPENDENCIES)
                .withTags(TAGS)
                .withClusterCriteria(CLUSTER_CRITERIA)
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .body(CLUSTER_CRITERIA_PATH, Matchers.hasSize(CLUSTER_CRITERIA.size()))
            .body(
                CLUSTER_CRITERIA_PATH + "[0].id",
                Matchers.is(CLUSTER_CRITERIA.get(0).getId().orElse(null))
            )
            .body(
                CLUSTER_CRITERIA_PATH + "[0].name",
                Matchers.is(CLUSTER_CRITERIA.get(0).getName().orElse(null))
            )
            .body(
                CLUSTER_CRITERIA_PATH + "[0].version",
                Matchers.is(CLUSTER_CRITERIA.get(0).getVersion().orElse(null))
            )
            .body(
                CLUSTER_CRITERIA_PATH + "[0].status",
                Matchers.is(CLUSTER_CRITERIA.get(0).getStatus().orElse(null))
            )
            .body(
                CLUSTER_CRITERIA_PATH + "[0].tags",
                Matchers.containsInAnyOrder(CLUSTER_CRITERIA.get(0).getTags().toArray(new String[0]))
            )
            .body(
                CLUSTER_CRITERIA_PATH + "[1].id",
                Matchers.is(CLUSTER_CRITERIA.get(1).getId().orElse(null))
            )
            .body(
                CLUSTER_CRITERIA_PATH + "[1].name",
                Matchers.is(CLUSTER_CRITERIA.get(1).getName().orElse(null))
            )
            .body(
                CLUSTER_CRITERIA_PATH + "[1].version",
                Matchers.is(CLUSTER_CRITERIA.get(1).getVersion().orElse(null))
            )
            .body(
                CLUSTER_CRITERIA_PATH + "[1].status",
                Matchers.is(CLUSTER_CRITERIA.get(1).getStatus().orElse(null))
            )
            .body(
                CLUSTER_CRITERIA_PATH + "[1].tags",
                Matchers.containsInAnyOrder(CLUSTER_CRITERIA.get(1).getTags().toArray(new String[0]))
            )
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

        Assertions.assertThat(this.commandRepository.count()).isEqualTo(1L);
    }

    @Test
    void canCreateCommandWithId() throws Exception {
        this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(ID))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(NAME))
            .body(USER_PATH, Matchers.is(USER))
            .body(VERSION_PATH, Matchers.is(VERSION))
            .body(STATUS_PATH, Matchers.is(CommandStatus.ACTIVE.toString()))
            .body(EXECUTABLE_PATH, Matchers.is(EXECUTABLE))
            .body(EXECUTABLE_AND_ARGS_PATH, Matchers.is(EXECUTABLE_AND_ARGS))
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
            .body(CLUSTER_CRITERIA_PATH, Matchers.hasSize(0))
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

        Assertions.assertThat(this.commandRepository.count()).isEqualTo(1L);
    }

    @Test
    void canHandleBadInputToCreateCommand() throws Exception {
        final Command cluster =
            new Command.Builder(" ", " ", " ", CommandStatus.ACTIVE, Lists.newArrayList(""), -1L).build();
        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(cluster))
            .when()
            .port(this.port)
            .post(COMMANDS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()))
            .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
            .body(
                EXCEPTION_MESSAGE_PATH,
                Matchers.containsString("must not be empty"
                )
            );

        Assertions.assertThat(this.commandRepository.count()).isEqualTo(0L);
    }

    @Test
    void canFindCommands() throws Exception {
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

        this.createConfigResource(
            new Command
                .Builder(name1, user1, version1, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withId(id1)
                .build(),
            null
        );
        Thread.sleep(1000);
        this.createConfigResource(
            new Command
                .Builder(name2, user2, version2, CommandStatus.DEPRECATED, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withId(id2)
                .build(),
            null
        );
        Thread.sleep(1000);
        this.createConfigResource(
            new Command
                .Builder(name3, user3, version3, CommandStatus.INACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(COMMANDS_LIST_PATH, Matchers.hasSize(1))
            .body(COMMANDS_LIST_PATH + "[0].id", Matchers.is(id1));

        //TODO: Add tests for sort, orderBy etc

        Assertions.assertThat(this.commandRepository.count()).isEqualTo(3L);
    }

    @Test
    void canUpdateCommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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
                    .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
                    .extract()
                    .asByteArray(),
                new TypeReference<EntityModel<Command>>() {
                }
            ).getContent();
        Assertions.assertThat(createdCommand).isNotNull();
        Assertions.assertThat(createdCommand.getStatus()).isEqualByComparingTo(CommandStatus.ACTIVE);

        final Command.Builder updateCommand = new Command.Builder(
            createdCommand.getName(),
            createdCommand.getUser(),
            createdCommand.getVersion(),
            CommandStatus.INACTIVE,
            createdCommand.getExecutableAndArguments(),
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(STATUS_PATH, Matchers.is(CommandStatus.INACTIVE.toString()));

        Assertions.assertThat(this.commandRepository.count()).isEqualTo(1L);
    }

    @Test
    void canPatchCommand() throws Exception {
        final String id = this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(NAME_PATH, Matchers.is(newName));

        Assertions.assertThat(this.commandRepository.count()).isEqualTo(1L);
    }

    @Test
    void canDeleteAllCommands() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.DEPRECATED, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.INACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .build(),
            null
        );
        Assertions.assertThat(this.commandRepository.count()).isEqualTo(3L);

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

        Assertions.assertThat(this.commandRepository.count()).isEqualTo(0L);
    }

    @Test
    void canDeleteACommand() throws Exception {
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

        this.createConfigResource(
            new Command
                .Builder(name1, user1, version1, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withId(id1)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(name2, user2, version2, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withId(id2)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(name3, user3, version3, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withId(id3)
                .build(),
            null
        );
        Assertions.assertThat(this.commandRepository.count()).isEqualTo(3L);

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
            .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()))
            .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
            .body(
                EXCEPTION_MESSAGE_PATH,
                Matchers.containsString("No command with id"
                )
            );

        Assertions.assertThat(this.commandRepository.count()).isEqualTo(2L);
    }

    @Test
    void canAddConfigsToCommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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

    @Test
    void canUpdateConfigsForCommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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

    @Test
    void canDeleteConfigsForCommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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

    @Test
    void canAddDependenciesToCommand() throws Exception {
        this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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

    @Test
    void canUpdateDependenciesForCommand() throws Exception {
        this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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

    @Test
    void canDeleteDependenciesForCommand() throws Exception {
        this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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

    @Test
    void canAddTagsToCommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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

    @Test
    void canUpdateTagsForCommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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

    @Test
    void canDeleteTagsForCommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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

    @Test
    void canDeleteTagForCommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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

    @Test
    void canAddApplicationsForACommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()))
            .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
            .body(EXCEPTION_MESSAGE_PATH, Matchers.notNullValue());

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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.hasSize(3))
            .body("[0].id", Matchers.is(applicationId1))
            .body("[1].id", Matchers.is(applicationId2))
            .body("[2].id", Matchers.is(applicationId3));
    }

    @Test
    void canSetApplicationsForACommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.empty());
    }

    @Test
    void canRemoveApplicationsFromACommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.empty());
    }

    @Test
    void canRemoveApplicationFromACommand() throws Exception {
        this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
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
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.hasSize(1))
            .body("[0].id", Matchers.is(ID));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(APPLICATIONS_API + "/{id}/commands", applicationId2)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.empty());

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(APPLICATIONS_API + "/{id}/commands", applicationId3)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.hasSize(1))
            .body("[0].id", Matchers.is(ID));
    }

    @Test
    void canGetClustersForCommand() throws Exception {
        final String placeholder = UUID.randomUUID().toString();
        final String cluster1Id = this.createConfigResource(
            new Cluster.Builder(placeholder, placeholder, placeholder, ClusterStatus.UP).build(),
            null
        );
        final String cluster2Id = this.createConfigResource(
            new Cluster.Builder(placeholder, placeholder, placeholder, ClusterStatus.OUT_OF_SERVICE).build(),
            null
        );
        final String cluster3Id = this.createConfigResource(
            new Cluster
                .Builder(placeholder, placeholder, placeholder, ClusterStatus.TERMINATED)
                .build(),
            null
        );

        final String commandId = this.createConfigResource(
            new Command
                .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withClusterCriteria(
                    Lists.newArrayList(
                        new Criterion.Builder().withId(cluster1Id).build(),
                        new Criterion.Builder().withId(cluster2Id).build(),
                        new Criterion.Builder().withId(cluster3Id).build()
                    )
                )
                .build(),
            null
        );

        Assertions
            .assertThat(
                Arrays.stream(
                    GenieObjectMapper.getMapper().readValue(
                        RestAssured
                            .given(this.getRequestSpecification())
                            .when()
                            .port(this.port)
                            .get(COMMANDS_API + "/{id}/clusters", commandId)
                            .then()
                            .statusCode(Matchers.is(HttpStatus.OK.value()))
                            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
                            .extract()
                            .asByteArray(),
                        new TypeReference<EntityModel<Cluster>[]>() {
                        }
                    )
                )
                    .map(EntityModel::getContent)
                    .filter(Objects::nonNull)
                    .map(Cluster::getId)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList())
            )
            .containsExactlyInAnyOrder(cluster1Id, cluster2Id, cluster3Id);

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
            .get(COMMANDS_API + "/{id}/clusters", commandId)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.hasSize(1))
            .body("[0].id", Matchers.is(cluster1Id));
    }

    @Test
    void canCreateCommandWithBlankFields() throws Exception {
        final Set<String> stringSetWithBlank = Sets.newHashSet("foo", " ");

        final List<Command> invalidCommandResources = Lists.newArrayList(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withId(UUID.randomUUID().toString())
                .withSetupFile(" ")
                .build(),

            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withId(UUID.randomUUID().toString())
                .withConfigs(stringSetWithBlank)
                .build(),

            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withId(UUID.randomUUID().toString())
                .withDependencies(stringSetWithBlank)

                .build(),

            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withId(UUID.randomUUID().toString())
                .withTags(stringSetWithBlank)
                .build()
        );

        long i = 0L;
        for (final Command invalidCommandResource : invalidCommandResources) {
            Assertions.assertThat(this.commandRepository.count()).isEqualTo(i);

            RestAssured
                .given(this.getRequestSpecification())
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(GenieObjectMapper.getMapper().writeValueAsBytes(invalidCommandResource))
                .when()
                .port(this.port)
                .post(COMMANDS_API)
                .then()
                .statusCode(Matchers.is(HttpStatus.CREATED.value()));

            Assertions.assertThat(this.commandRepository.count()).isEqualTo(++i);
        }
    }

    @Test
    void testCommandNotFound() {
        final List<String> paths = Lists.newArrayList("", "/applications", "/clusters", "/clusterCriteria");

        for (final String relationPath : paths) {
            RestAssured
                .given(this.getRequestSpecification())
                .when()
                .port(this.port)
                .get(COMMANDS_API + "/{id}" + relationPath, ID)
                .then()
                .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()))
                .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
                .body(EXCEPTION_MESSAGE_PATH, Matchers.is("No command with id " + ID + " exists"));
        }
    }

    @Test
    void testGetClusterCriteria() throws Exception {
        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.JSON_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getClusterCriteriaForCommandResponsePayload() // Response Fields
        );

        final String id = this.createCommandWithDefaultClusterCriteria();

        // Don't use the helper method as we want to document this call this time
        final List<Criterion> clusterCriteria = RestAssured
            .given(this.getRequestSpecification())
            .filter(getFilter)
            .when()
            .port(this.port)
            .get(COMMANDS_API + "/{id}/clusterCriteria", id)
            .then()
            .statusCode(HttpStatus.OK.value())
            .contentType(ContentType.JSON)
            .extract()
            .jsonPath()
            .getList(".", Criterion.class);

        Assertions.assertThat(clusterCriteria).isEqualTo(CLUSTER_CRITERIA);
    }

    @Test
    void testRemoveAllClusterCriteriaFromCommand() throws Exception {
        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM // Path parameters
        );

        final String id = this.createCommandWithDefaultClusterCriteria();

        RestAssured
            .given(this.getRequestSpecification())
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(COMMANDS_API + "/{id}/clusterCriteria", id)
            .then()
            .statusCode(HttpStatus.OK.value());

        Assertions.assertThat(this.getClusterCriteria(id)).isEmpty();
    }

    @Test
    void testAddLowestPriorityClusterCriterion() throws Exception {
        final RestDocumentationFilter addFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // Path parameters,
            Snippets.CONTENT_TYPE_HEADER, // Request Header
            Snippets.addClusterCriterionForCommandRequestPayload() // Payload Docs
        );

        final String id = this.createCommandWithDefaultClusterCriteria();
        final Criterion newCriterion = new Criterion
            .Builder()
            .withVersion("3.0.0")
            .withId(UUID.randomUUID().toString())
            .withName("adhocCluster")
            .withStatus(ClusterStatus.UP.name())
            .withTags(Sets.newHashSet("sched:adhoc", "type:yarn"))
            .build();

        RestAssured
            .given(this.getRequestSpecification())
            .filter(addFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(newCriterion))
            .when()
            .port(this.port)
            .post(COMMANDS_API + "/{id}/clusterCriteria", id)
            .then()
            .statusCode(HttpStatus.OK.value());

        Assertions
            .assertThat(this.getClusterCriteria(id))
            .hasSize(CLUSTER_CRITERIA.size() + 1)
            .containsSequence(CLUSTER_CRITERIA)
            .last()
            .isEqualTo(newCriterion);
    }

    @Test
    void testSetClusterCriteriaForCommand() throws Exception {
        final RestDocumentationFilter setFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // Path parameters,
            Snippets.CONTENT_TYPE_HEADER, // Request Header
            Snippets.setClusterCriteriaForCommandRequestPayload() // Payload Docs
        );

        final String id = this.createCommandWithDefaultClusterCriteria();
        final List<Criterion> newCriteria = Lists.newArrayList(
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withVersion(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withName(UUID.randomUUID().toString()).build()
        );
        Assertions.assertThat(newCriteria).doesNotContainAnyElementsOf(CLUSTER_CRITERIA);

        RestAssured
            .given(this.getRequestSpecification())
            .filter(setFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(newCriteria))
            .when()
            .port(this.port)
            .put(COMMANDS_API + "/{id}/clusterCriteria", id)
            .then()
            .statusCode(HttpStatus.OK.value());

        Assertions.assertThat(this.getClusterCriteria(id)).isEqualTo(newCriteria);
    }

    @Test
    void testInsertClusterCriterionForCommand() throws Exception {
        final RestDocumentationFilter insertFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            // Path parameters
            Snippets
                .ID_PATH_PARAM
                .and(
                    RequestDocumentation
                        .parameterWithName("priority")
                        .description("Priority of the criterion to insert")
                ),
            Snippets.CONTENT_TYPE_HEADER, // Request Header
            Snippets.addClusterCriterionForCommandRequestPayload() // Payload Docs
        );

        final String id = this.createCommandWithDefaultClusterCriteria();
        final Criterion newCriterion = new Criterion
            .Builder()
            .withVersion("4.0.0")
            .withId(UUID.randomUUID().toString())
            .withName("insightCluster")
            .withStatus(ClusterStatus.OUT_OF_SERVICE.name())
            .withTags(Sets.newHashSet("sched:insights", "type:presto"))
            .build();

        RestAssured
            .given(this.getRequestSpecification())
            .filter(insertFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(newCriterion))
            .when()
            .port(this.port)
            .put(COMMANDS_API + "/{id}/clusterCriteria/{priority}", id, 1)
            .then()
            .statusCode(HttpStatus.OK.value());

        Assertions
            .assertThat(this.getClusterCriteria(id))
            .hasSize(CLUSTER_CRITERIA.size() + 1)
            .containsExactly(CLUSTER_CRITERIA.get(0), newCriterion, CLUSTER_CRITERIA.get(1));
    }

    @Test
    void testRemoveClusterCriterionFromCommand() throws Exception {
        final RestDocumentationFilter removeFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            // Path parameters
            Snippets
                .ID_PATH_PARAM
                .and(
                    RequestDocumentation
                        .parameterWithName("priority")
                        .description("Priority of the criterion to insert")
                )
        );

        final String id = this.createCommandWithDefaultClusterCriteria();

        RestAssured
            .given(this.getRequestSpecification())
            .filter(removeFilter)
            .when()
            .port(this.port)
            .delete(COMMANDS_API + "/{id}/clusterCriteria/{priority}", id, 1)
            .then()
            .statusCode(HttpStatus.OK.value());

        Assertions
            .assertThat(this.getClusterCriteria(id))
            .hasSize(CLUSTER_CRITERIA.size() - 1)
            .containsExactly(CLUSTER_CRITERIA.get(0));

        // Running again throws 404
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .delete(COMMANDS_API + "/{id}/clusterCriteria/{priority}", id, 1)
            .then()
            .statusCode(HttpStatus.NOT_FOUND.value());
    }

    @Test
    void testResolveClustersForCommandClusterCriteria() throws Exception {
        final Cluster cluster0 = new Cluster.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            ClusterStatus.UP
        )
            .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            .build();
        final Cluster cluster1 = new Cluster.Builder(
            cluster0.getName(),
            cluster0.getUser(),
            cluster0.getVersion(),
            ClusterStatus.UP
        )
            .withTags(cluster0.getTags())
            .build();
        final Cluster cluster2 = new Cluster.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            ClusterStatus.UP
        )
            .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            .build();
        final String cluster0Id = this.createConfigResource(cluster0, null);
        final String cluster1Id = this.createConfigResource(cluster1, null);
        final String cluster2Id = this.createConfigResource(cluster2, null);

        final Command command = new Command
            .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
            .withClusterCriteria(
                Lists.newArrayList(
                    new Criterion.Builder().withId(cluster0Id).build(),
                    new Criterion.Builder().withName(cluster1.getName()).build(),
                    new Criterion.Builder().withVersion(cluster2.getVersion()).build(),
                    new Criterion.Builder().withStatus(ClusterStatus.TERMINATED.name()).build(),
                    new Criterion.Builder().withTags(cluster2.getTags()).build()
                )
            )
            .build();
        final String commandId = this.createConfigResource(command, null);

        final RestDocumentationFilter resolveFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            // Path parameters
            Snippets.ID_PATH_PARAM,
            // Request parameters
            RequestDocumentation.requestParameters(
                RequestDocumentation
                    .parameterWithName("addDefaultStatus")
                    .description(
                        "Whether the system should add the default cluster status to the criteria. Default: true"
                    )
                    .optional()
            ),
            // Response Content Type
            Snippets.JSON_CONTENT_TYPE_HEADER,
            // Response Fields
            Snippets.resolveClustersForCommandClusterCriteriaResponsePayload()
        );

        final List<ResolvedResources<Cluster>> resolvedClusters = GenieObjectMapper.getMapper().readValue(
            RestAssured
                .given(this.getRequestSpecification())
                .filter(resolveFilter)
                .when()
                .port(this.port)
                .get(COMMANDS_API + "/{id}/resolvedClusters", commandId)
                .then()
                .statusCode(HttpStatus.OK.value())
                .contentType(Matchers.containsString(MediaType.APPLICATION_JSON_VALUE))
                .extract()
                .asString(),
            new TypeReference<List<ResolvedResources<Cluster>>>() {
            }
        );

        Assertions.assertThat(resolvedClusters).hasSize(5);
        ResolvedResources<Cluster> resolvedResources = resolvedClusters.get(0);
        Assertions.assertThat(resolvedResources.getCriterion()).isEqualTo(command.getClusterCriteria().get(0));
        Assertions
            .assertThat(resolvedResources.getResources())
            .extracting(Cluster::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactlyInAnyOrder(cluster0Id);

        resolvedResources = resolvedClusters.get(1);
        Assertions.assertThat(resolvedResources.getCriterion()).isEqualTo(command.getClusterCriteria().get(1));
        Assertions
            .assertThat(resolvedResources.getResources())
            .extracting(Cluster::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactlyInAnyOrder(cluster0Id, cluster1Id);

        resolvedResources = resolvedClusters.get(2);
        Assertions.assertThat(resolvedResources.getCriterion()).isEqualTo(command.getClusterCriteria().get(2));
        Assertions
            .assertThat(resolvedResources.getResources())
            .extracting(Cluster::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactlyInAnyOrder(cluster2Id);

        resolvedResources = resolvedClusters.get(3);
        Assertions.assertThat(resolvedResources.getCriterion()).isEqualTo(command.getClusterCriteria().get(3));
        Assertions.assertThat(resolvedResources.getResources()).isEmpty();

        resolvedResources = resolvedClusters.get(4);
        Assertions.assertThat(resolvedResources.getCriterion()).isEqualTo(command.getClusterCriteria().get(4));
        Assertions
            .assertThat(resolvedResources.getResources())
            .extracting(Cluster::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactlyInAnyOrder(cluster2Id);
    }

    private String createCommandWithDefaultClusterCriteria() throws Exception {
        return this.createConfigResource(
            new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
                .withClusterCriteria(CLUSTER_CRITERIA)
                .build(),
            null
        );
    }

    private List<Criterion> getClusterCriteria(final String id) {
        return RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(COMMANDS_API + "/{id}/clusterCriteria", id)
            .then()
            .statusCode(HttpStatus.OK.value())
            .contentType(ContentType.JSON)
            .extract()
            .jsonPath()
            .getList(".", Criterion.class);
    }
}
