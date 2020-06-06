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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import io.restassured.RestAssured;
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
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the Applications REST API.
 *
 * @author tgianos
 * @since 3.0.0
 */
class ApplicationRestControllerIntegrationTest extends RestControllerIntegrationTestBase {

    // Use a `.` to ensure that the Spring prefix matcher is turned off
    // see: https://tinyurl.com/yblzglk8
    private static final String ID = UUID.randomUUID().toString() + "." + UUID.randomUUID().toString();
    private static final String NAME = "spark";
    private static final String USER = "genie";
    private static final String VERSION = "1.5.1";
    private static final String TYPE = "spark";

    private static final String TYPE_PATH = "type";

    private static final String APPLICATIONS_LIST_PATH = EMBEDDED_PATH + ".applicationList";
    private static final String APPLICATIONS_ID_LIST_PATH = APPLICATIONS_LIST_PATH + ".id";
    private static final String APPLICATION_COMMANDS_LINK_PATH = "_links.commands.href";
    private static final String APPLICATIONS_COMMANDS_LINK_PATH = APPLICATIONS_LIST_PATH + "._links.commands.href";
    private static final List<String> EXECUTABLE_AND_ARGS = Lists.newArrayList("bash");

    @BeforeEach
    void beforeApplications() {
        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(0L);
    }

    @Test
    void canCreateApplicationWithoutId() throws Exception {
        final RestDocumentationFilter creationResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request headers
            Snippets.getApplicationRequestPayload(), // Request fields
            Snippets.LOCATION_HEADER // Response headers
        );

        final String id = this.createConfigResource(
            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withType(TYPE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/" + VERSION + "/spark.tar.gz"))
                .withSetupFile("s3://mybucket/spark/" + VERSION + "/setupBase-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/" + VERSION + "/spark-env.sh"))
                .withDescription("Spark for Genie")
                .withTags(Sets.newHashSet("type:" + TYPE, "ver:" + VERSION))
                .build(),
            creationResultFilter
        );

        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // response headers
            Snippets.getApplicationResponsePayload(), // response payload
            Snippets.APPLICATION_LINKS // response links
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(APPLICATIONS_API + "/{id}", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(NAME))
            .body(VERSION_PATH, Matchers.is(VERSION))
            .body(USER_PATH, Matchers.is(USER))
            .body(DESCRIPTION_PATH, Matchers.is("Spark for Genie"))
            .body(SETUP_FILE_PATH, Matchers.is("s3://mybucket/spark/" + VERSION + "/setupBase-spark.sh"))
            .body(CONFIGS_PATH, Matchers.hasItem("s3://mybucket/spark/" + VERSION + "/spark-env.sh"))
            .body(TAGS_PATH, Matchers.hasSize(4))
            .body(TAGS_PATH, Matchers.hasItem("genie.id:" + id))
            .body(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME))
            .body(TAGS_PATH, Matchers.hasItem("ver:" + VERSION))
            .body(TAGS_PATH, Matchers.hasItem("type:" + TYPE))
            .body(STATUS_PATH, Matchers.is(ApplicationStatus.ACTIVE.toString()))
            .body(DEPENDENCIES_PATH, Matchers.hasItem("s3://mybucket/spark/" + VERSION + "/spark.tar.gz"))
            .body(TYPE_PATH, Matchers.is(TYPE))
            .body(LINKS_PATH + ".keySet().size()", Matchers.is(2))
            .body(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY))
            .body(APPLICATION_COMMANDS_LINK_PATH, EntityLinkMatcher
                .matchUri(
                    APPLICATIONS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS,
                    id
                )
            );

        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(1L);
    }

    @Test
    void canCreateApplicationWithId() throws Exception {
        this.createConfigResource(
            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withId(ID)
                .withType(TYPE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/" + VERSION + "/spark.tar.gz"))
                .withSetupFile("s3://mybucket/spark/" + VERSION + "/setupBase-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/" + VERSION + "/spark-env.sh"))
                .withDescription("Spark for Genie")
                .withTags(Sets.newHashSet("type:" + TYPE, "ver:" + VERSION))
                .build(),
            null
        );

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(APPLICATIONS_API + "/{id}", ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(ID))
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(NAME))
            .body(VERSION_PATH, Matchers.is(VERSION))
            .body(USER_PATH, Matchers.is(USER))
            .body(DESCRIPTION_PATH, Matchers.is("Spark for Genie"))
            .body(SETUP_FILE_PATH, Matchers.is("s3://mybucket/spark/" + VERSION + "/setupBase-spark.sh"))
            .body(CONFIGS_PATH, Matchers.hasItem("s3://mybucket/spark/" + VERSION + "/spark-env.sh"))
            .body(TAGS_PATH, Matchers.hasSize(4))
            .body(TAGS_PATH, Matchers.hasItem("genie.id:" + ID))
            .body(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME))
            .body(TAGS_PATH, Matchers.hasItem("ver:" + VERSION))
            .body(TAGS_PATH, Matchers.hasItem("type:" + TYPE))
            .body(STATUS_PATH, Matchers.is(ApplicationStatus.ACTIVE.toString()))
            .body(DEPENDENCIES_PATH, Matchers.hasItem("s3://mybucket/spark/" + VERSION + "/spark.tar.gz"))
            .body(TYPE_PATH, Matchers.is(TYPE))
            .body(LINKS_PATH + ".keySet().size()", Matchers.is(2))
            .body(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY))
            .body(
                APPLICATION_COMMANDS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    APPLICATIONS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS,
                    ID
                )
            );

        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(1L);
    }

    @Test
    void canHandleBadInputToCreateApplication() throws Exception {
        final Application app = new Application.Builder(" ", " ", " ", ApplicationStatus.ACTIVE).build();

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(app))
            .when()
            .port(this.port)
            .post(APPLICATIONS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()))
            .contentType(Matchers.containsString(MediaType.APPLICATION_JSON_VALUE))
            .body(
                EXCEPTION_MESSAGE_PATH,
                Matchers.containsString("A version is required and must be at most 255 characters")
            );

        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(0L);
    }

    @Test
    void canFindApplications() throws Exception {
        final Application spark151 = new Application.Builder("spark", "genieUser1", "1.5.1", ApplicationStatus.ACTIVE)
            .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.5.1.tar.gz"))
            .withSetupFile("s3://mybucket/spark/setupBase-spark.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
            .withDescription("Spark 1.5.1 for Genie")
            .withTags(Sets.newHashSet("type:spark", "ver:1.5.1"))
            .withType("spark")
            .build();

        final Application spark150 = new Application.Builder("spark", "genieUser2", "1.5.0", ApplicationStatus.ACTIVE)
            .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.5.0.tar.gz"))
            .withSetupFile("s3://mybucket/spark/setupBase-spark.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
            .withDescription("Spark 1.5.0 for Genie")
            .withTags(Sets.newHashSet("type:spark", "ver:1.5.0"))
            .withType("spark")
            .build();

        final Application spark141 = new Application.Builder("spark", "genieUser3", "1.4.1", ApplicationStatus.INACTIVE)
            .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.4.1.tar.gz"))
            .withSetupFile("s3://mybucket/spark/setupBase-spark.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
            .withDescription("Spark 1.4.1 for Genie")
            .withTags(Sets.newHashSet("type:spark", "ver:1.4.1"))
            .withType("spark")
            .build();

        final Application spark140
            = new Application.Builder("spark", "genieUser4", "1.4.0", ApplicationStatus.DEPRECATED)
            .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.4.0.tar.gz"))
            .withSetupFile("s3://mybucket/spark/setupBase-spark.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
            .withDescription("Spark 1.4.0 for Genie")
            .withTags(Sets.newHashSet("type:spark", "ver:1.4.0"))
            .withType("spark")
            .build();

        final Application spark131
            = new Application.Builder("spark", "genieUser5", "1.3.1", ApplicationStatus.DEPRECATED)
            .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.3.1.tar.gz"))
            .withSetupFile("s3://mybucket/spark/setupBase-spark.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
            .withDescription("Spark 1.3.1 for Genie")
            .withTags(Sets.newHashSet("type:spark", "ver:1.3.1"))
            .withType("spark")
            .build();

        final Application pig = new Application.Builder("spark", "genieUser6", "0.4.0", ApplicationStatus.ACTIVE)
            .withDependencies(Sets.newHashSet("s3://mybucket/pig/pig-0.15.0.tar.gz"))
            .withSetupFile("s3://mybucket/pig/setupBase-pig.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/pig/pig.properties"))
            .withDescription("Pig 0.15.0 for Genie")
            .withTags(Sets.newHashSet("type:pig", "ver:0.15.0"))
            .withType("pig")
            .build();

        final Application hive = new Application.Builder("hive", "genieUser7", "1.0.0", ApplicationStatus.ACTIVE)
            .withDependencies(Sets.newHashSet("s3://mybucket/hive/hive-1.0.0.tar.gz"))
            .withSetupFile("s3://mybucket/hive/setupBase-hive.sh")
            .withConfigs(
                Sets.newHashSet("s3://mybucket/hive/hive-env.sh", "s3://mybucket/hive/hive-log4j.properties")
            )
            .withDescription("Hive 1.0.0 for Genie")
            .withTags(Sets.newHashSet("type:hive", "ver:1.0.0"))
            .withType("hive")
            .build();

        final String spark151Id = this.createConfigResource(spark151, null);
        final String spark150Id = this.createConfigResource(spark150, null);
        final String spark141Id = this.createConfigResource(spark141, null);
        final String spark140Id = this.createConfigResource(spark140, null);
        final String spark131Id = this.createConfigResource(spark131, null);
        final String pigId = this.createConfigResource(pig, null);
        final String hiveId = this.createConfigResource(hive, null);

        final List<String> appIds = Lists.newArrayList(
            spark151Id, spark150Id, spark141Id, spark140Id, spark131Id, pigId, hiveId);

        final RestDocumentationFilter findFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.APPLICATION_SEARCH_QUERY_PARAMETERS, // Request query parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response headers
            Snippets.APPLICATION_SEARCH_RESULT_FIELDS, // Result fields
            Snippets.SEARCH_LINKS // HAL Links
        );

        // Test finding all applications
        RestAssured
            .given(this.getRequestSpecification()).filter(findFilter)
            .when()
            .port(this.port).get(APPLICATIONS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(APPLICATIONS_LIST_PATH, Matchers.hasSize(7))
            .body(APPLICATIONS_ID_LIST_PATH, Matchers.hasSize(7))
            .body(
                APPLICATIONS_ID_LIST_PATH,
                Matchers.containsInAnyOrder(spark151Id, spark150Id, spark141Id, spark140Id, spark131Id, pigId, hiveId)
            )
            .body(
                APPLICATIONS_COMMANDS_LINK_PATH,
                EntitiesLinksMatcher.matchUrisAnyOrder(
                    APPLICATIONS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS, appIds
                )
            );

        // Limit the size
        RestAssured
            .given(this.getRequestSpecification()).filter(findFilter).param("size", 2)
            .when()
            .port(this.port)
            .get(APPLICATIONS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(APPLICATIONS_LIST_PATH, Matchers.hasSize(2));

        // Query by name
        RestAssured
            .given(this.getRequestSpecification()).filter(findFilter).param("name", "hive")
            .when()
            .port(this.port)
            .get(APPLICATIONS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(APPLICATIONS_LIST_PATH, Matchers.hasSize(1))
            .body(APPLICATIONS_LIST_PATH + "[0].id", Matchers.is(hiveId));

        // Query by user
        RestAssured
            .given(this.getRequestSpecification()).filter(findFilter).param("user", "genieUser3")
            .when()
            .port(this.port)
            .get(APPLICATIONS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(APPLICATIONS_LIST_PATH, Matchers.hasSize(1))
            .body(APPLICATIONS_LIST_PATH + "[0].id", Matchers.is(spark141Id));

        // Query by statuses
        RestAssured
            .given(this.getRequestSpecification()).filter(findFilter)
            .param("status", ApplicationStatus.ACTIVE.toString(), ApplicationStatus.DEPRECATED.toString())
            .when()
            .port(this.port)
            .get(APPLICATIONS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(APPLICATIONS_LIST_PATH, Matchers.hasSize(6))
            .body(APPLICATIONS_LIST_PATH + "[0].id", Matchers.is(hiveId))
            .body(APPLICATIONS_LIST_PATH + "[1].id", Matchers.is(pigId))
            .body(APPLICATIONS_LIST_PATH + "[2].id", Matchers.is(spark131Id))
            .body(APPLICATIONS_LIST_PATH + "[3].id", Matchers.is(spark140Id))
            .body(APPLICATIONS_LIST_PATH + "[4].id", Matchers.is(spark150Id))
            .body(APPLICATIONS_LIST_PATH + "[5].id", Matchers.is(spark151Id));

        // Query by tags
        RestAssured
            .given(this.getRequestSpecification()).filter(findFilter).param("tag", "genie.id:" + spark131Id)
            .when()
            .port(this.port)
            .get(APPLICATIONS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(APPLICATIONS_LIST_PATH, Matchers.hasSize(1))
            .body(APPLICATIONS_LIST_PATH + "[0].id", Matchers.is(spark131Id));

        // Query by type
        RestAssured
            .given(this.getRequestSpecification()).filter(findFilter).param("type", "spark")
            .when()
            .port(this.port)
            .get(APPLICATIONS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(APPLICATIONS_LIST_PATH, Matchers.hasSize(5))
            .body(APPLICATIONS_LIST_PATH + "[0].id", Matchers.is(spark131Id))
            .body(APPLICATIONS_LIST_PATH + "[1].id", Matchers.is(spark140Id))
            .body(APPLICATIONS_LIST_PATH + "[2].id", Matchers.is(spark141Id))
            .body(APPLICATIONS_LIST_PATH + "[3].id", Matchers.is(spark150Id))
            .body(APPLICATIONS_LIST_PATH + "[4].id", Matchers.is(spark151Id));

        //TODO: Add tests for sort, orderBy etc

        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(7L);
    }

    @Test
    void canUpdateApplication() throws Exception {
        final String id = this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String applicationResource = APPLICATIONS_API + "/{id}";
        final Application createdApp = GenieObjectMapper.getMapper()
            .readValue(
                RestAssured
                    .given(this.getRequestSpecification())
                    .when()
                    .port(this.port)
                    .get(applicationResource, ID)
                    .then()
                    .statusCode(Matchers.is(HttpStatus.OK.value()))
                    .extract()
                    .asByteArray(),
                new TypeReference<EntityModel<Application>>() {
                }
            ).getContent();
        Assertions.assertThat(createdApp).isNotNull();
        Assertions.assertThat(createdApp.getStatus()).isEqualByComparingTo(ApplicationStatus.ACTIVE);

        final Application.Builder newStatusApp = new Application.Builder(
            createdApp.getName(),
            createdApp.getUser(),
            createdApp.getVersion(),
            ApplicationStatus.INACTIVE
        )
            .withId(createdApp.getId().orElseThrow(IllegalArgumentException::new))
            .withCreated(createdApp.getCreated().orElseThrow(IllegalArgumentException::new))
            .withUpdated(createdApp.getUpdated().orElseThrow(IllegalArgumentException::new))
            .withTags(createdApp.getTags())
            .withConfigs(createdApp.getConfigs())
            .withDependencies(createdApp.getDependencies());

        createdApp.getDescription().ifPresent(newStatusApp::withDescription);
        createdApp.getSetupFile().ifPresent(newStatusApp::withSetupFile);

        final RestDocumentationFilter updateResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // request header
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.getApplicationRequestPayload() // payload fields
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(updateResultFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(newStatusApp.build()))
            .when()
            .port(this.port)
            .put(applicationResource, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(applicationResource, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(STATUS_PATH, Matchers.is(ApplicationStatus.INACTIVE.toString()));

        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(1L);
    }

    @Test
    void canPatchApplication() throws Exception {
        final String id = this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String applicationResource = APPLICATIONS_API + "/{id}";
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(applicationResource, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(USER_PATH, Matchers.is(USER));

        final String newUser = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/user\", \"value\": \"" + newUser + "\" }]";
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
            .patch(applicationResource, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(applicationResource, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(USER_PATH, Matchers.is(newUser));

        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(1L);
    }

    @Test
    void canDeleteAllApplications() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).build(),
            null
        );
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.DEPRECATED).build(),
            null
        );
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.INACTIVE).build(),
            null
        );
        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(3L);

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/"
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(APPLICATIONS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(0L);
    }

    @Test
    void canDeleteAnApplication() throws Exception {
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
            new Application
                .Builder(name1, user1, version1, ApplicationStatus.ACTIVE)
                .withId(id1)
                .build(),
            null
        );
        this.createConfigResource(
            new Application
                .Builder(name2, user2, version2, ApplicationStatus.DEPRECATED)
                .withId(id2)
                .build(),
            null
        );
        this.createConfigResource(
            new Application
                .Builder(name3, user3, version3, ApplicationStatus.INACTIVE)
                .withId(id3)
                .build(),
            null
        );
        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(3L);

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM // path parameters
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(APPLICATIONS_API + "/{id}", id2)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(APPLICATIONS_API + "/{id}", id2)
            .then()
            .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()))
            .contentType(Matchers.containsString(MediaType.APPLICATION_JSON_VALUE))
            .body(
                EXCEPTION_MESSAGE_PATH,
                Matchers.containsString("No application with id")
            );

        Assertions.assertThat(this.applicationRepository.count()).isEqualTo(2L);
    }

    @Test
    void canAddConfigsToApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationFilter addFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // request headers
            Snippets.ID_PATH_PARAM, // path parameters
            PayloadDocumentation.requestFields(Snippets.CONFIG_FIELDS) // request payload fields
        );
        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.JSON_CONTENT_TYPE_HEADER, // response headers
            PayloadDocumentation.responseFields(Snippets.CONFIG_FIELDS) // response fields
        );
        this.canAddElementsToResource(APPLICATIONS_API + "/{id}/configs", ID, addFilter, getFilter);
    }

    @Test
    void canUpdateConfigsForApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.CONFIG_FIELDS) // Request fields
        );
        this.canUpdateElementsForResource(APPLICATIONS_API + "/{id}/configs", ID, updateFilter);
    }

    @Test
    void canDeleteConfigsForApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM // Path parameters
        );
        this.canDeleteElementsFromResource(APPLICATIONS_API + "/{id}/configs", ID, deleteFilter);
    }

    @Test
    void canAddDependenciesToApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
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
            APPLICATIONS_API + "/{id}/dependencies",
            ID,
            addFilter,
            getFilter
        );
    }

    @Test
    void canUpdateDependenciesForApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.DEPENDENCIES_FIELDS) // Request fields
        );
        this.canUpdateElementsForResource(
            APPLICATIONS_API + "/{id}/dependencies",
            ID,
            updateFilter
        );
    }

    @Test
    void canDeleteDependenciesForApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM // Path variables
        );
        this.canDeleteElementsFromResource(
            APPLICATIONS_API + "/{id}/dependencies",
            ID,
            deleteFilter
        );
    }

    @Test
    void canAddTagsToApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String api = APPLICATIONS_API + "/{id}/tags";

        final RestDocumentationFilter addFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // path params
            Snippets.CONTENT_TYPE_HEADER, // request header
            PayloadDocumentation.requestFields(Snippets.TAGS_FIELDS) // response fields
        );
        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.JSON_CONTENT_TYPE_HEADER, // response headers
            PayloadDocumentation.responseFields(Snippets.TAGS_FIELDS) // response fields
        );
        this.canAddTagsToResource(api, ID, NAME, addFilter, getFilter);
    }

    @Test
    void canUpdateTagsForApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String api = APPLICATIONS_API + "/{id}/tags";

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.TAGS_FIELDS) // Request fields
        );
        this.canUpdateTagsForResource(api, ID, NAME, updateFilter);
    }

    @Test
    void canDeleteTagsForApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String api = APPLICATIONS_API + "/{id}/tags";

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM
        );
        this.canDeleteTagsForResource(api, ID, NAME, deleteFilter);
    }

    @Test
    void canDeleteTagForApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String api = APPLICATIONS_API + "/{id}/tags";

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM.and(
                RequestDocumentation.parameterWithName("tag").description("The tag to remove")
            )
        );
        this.canDeleteTagForResource(api, ID, NAME, deleteFilter);
    }

    @Test
    void canGetCommandsForApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String placeholder = UUID.randomUUID().toString();
        final String command1Id = UUID.randomUUID().toString();
        final String command2Id = UUID.randomUUID().toString();
        final String command3Id = UUID.randomUUID().toString();
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, 1000L)
                .withId(command1Id)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.INACTIVE, EXECUTABLE_AND_ARGS, 1100L)
                .withId(command2Id)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.DEPRECATED, EXECUTABLE_AND_ARGS, 1200L)
                .withId(command3Id)
                .build(),
            null
        );

        final Set<String> appIds = Sets.newHashSet(ID);
        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(appIds))
            .when()
            .port(this.port)
            .post(COMMANDS_API + "/{id}/applications", command1Id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));
        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(appIds))
            .when()
            .port(this.port)
            .post(COMMANDS_API + "/{id}/applications", command3Id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        final String applicationCommandsAPI = APPLICATIONS_API + "/{id}/commands";

        Arrays.asList(
            GenieObjectMapper.getMapper().readValue(
                RestAssured
                    .given(this.getRequestSpecification())
                    .when()
                    .port(this.port)
                    .get(applicationCommandsAPI, ID)
                    .then()
                    .statusCode(Matchers.is(HttpStatus.OK.value()))
                    .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
                    .extract()
                    .asByteArray(),
                Command[].class
            )
        ).forEach(
            command -> {
                if (!command.getId().orElseThrow(IllegalArgumentException::new).equals(command1Id)
                    && !command.getId().orElseThrow(IllegalArgumentException::new).equals(command3Id)) {
                    Assertions.fail("Unexpected command");
                }
            }
        );

        // Filter by status
        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // Path parameters
            RequestDocumentation.requestParameters(
                RequestDocumentation
                    .parameterWithName("status")
                    .description("The status of commands to search for")
                    .attributes(
                        Attributes.key(Snippets.CONSTRAINTS).value(CommandStatus.values())
                    )
                    .optional()
            ), // Query Parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            PayloadDocumentation.responseFields(
                PayloadDocumentation
                    .subsectionWithPath("[]")
                    .description("The list of commands found")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            )
        );
        RestAssured
            .given(this.getRequestSpecification())
            .param("status", CommandStatus.ACTIVE.toString(), CommandStatus.INACTIVE.toString())
            .filter(getFilter)
            .when()
            .port(this.port)
            .get(applicationCommandsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.hasSize(1))
            .body("[0].id", Matchers.is(command1Id));
    }

    @Test
    void canCreateApplicationWithBlankFields() throws Exception {
        final Set<String> stringSetWithBlank = Sets.newHashSet("foo", " ");

        final List<Application> invalidApplicationResources = Lists.newArrayList(
            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withId(UUID.randomUUID().toString())
                .withSetupFile(" ")
                .build(),

            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withId(UUID.randomUUID().toString())
                .withConfigs(stringSetWithBlank)
                .build(),

            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withId(UUID.randomUUID().toString())
                .withDependencies(stringSetWithBlank)
                .build(),

            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withId(UUID.randomUUID().toString())
                .withTags(stringSetWithBlank)
                .build()
        );

        long i = 0L;
        for (final Application invalidApplicationResource : invalidApplicationResources) {
            Assertions.assertThat(this.applicationRepository.count()).isEqualTo(i);

            RestAssured
                .given(this.getRequestSpecification())
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(GenieObjectMapper.getMapper().writeValueAsBytes(invalidApplicationResource))
                .when()
                .port(this.port)
                .post(APPLICATIONS_API)
                .then()
                .statusCode(Matchers.is(HttpStatus.CREATED.value()));

            Assertions.assertThat(this.applicationRepository.count()).isEqualTo(++i);
        }
    }

    @Test
    void testApplicationNotFound() {
        final List<String> paths = Lists.newArrayList("", "/commands");

        for (final String path : paths) {
            RestAssured
                .given(this.getRequestSpecification())
                .when()
                .port(this.port)
                .get(APPLICATIONS_API + "/{id}" + path, ID)
                .then()
                .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()))
                .contentType(Matchers.containsString(MediaType.APPLICATION_JSON_VALUE))
                .body(EXCEPTION_MESSAGE_PATH, Matchers.startsWith("No application with id " + ID));
        }
    }
}
