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
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import io.restassured.RestAssured;
import org.apache.catalina.util.URLEncoder;
import org.apache.http.client.utils.URLEncodedUtils;
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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Integration tests for the Clusters REST API.
 *
 * @author tgianos
 * @since 3.0.0
 */
class ClusterRestControllerIntegrationTest extends RestControllerIntegrationTestBase {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = "h2prod";
    private static final String USER = "genie";
    private static final String VERSION = "2.7.1";

    private static final String CLUSTERS_LIST_PATH = EMBEDDED_PATH + ".clusterList";
    private static final String CLUSTERS_ID_LIST_PATH = CLUSTERS_LIST_PATH + ".id";
    private static final String CLUSTER_COMMANDS_LINK_PATH = "_links.commands.href";
    private static final String CLUSTERS_COMMANDS_LINK_PATH = CLUSTERS_LIST_PATH + "._links.commands.href";
    private static final List<String> EXECUTABLE_AND_ARGS = Lists.newArrayList("bash");

    @BeforeEach
    void beforeClusters() {
        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(0L);
    }

    @Test
    void canCreateClusterWithoutId() throws Exception {
        final RestDocumentationFilter createFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request headers
            Snippets.getClusterRequestPayload(), // Request fields
            Snippets.LOCATION_HEADER // Response headers
        );

        final String id = this.createConfigResource(
            new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).build(),
            createFilter
        );

        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // response headers
            Snippets.getClusterResponsePayload(), // response payload
            Snippets.CLUSTER_LINKS // response links
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getFilter)
            .when()
            .port(this.port)
            .get(CLUSTERS_API + "/{id}", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(NAME))
            .body(USER_PATH, Matchers.is(USER))
            .body(VERSION_PATH, Matchers.is(VERSION))
            .body(TAGS_PATH, Matchers.hasItem("genie.id:" + id))
            .body(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME))
            .body(SETUP_FILE_PATH, Matchers.nullValue())
            .body(STATUS_PATH, Matchers.is(ClusterStatus.UP.toString()))
            .body(CONFIGS_PATH, Matchers.hasSize(0))
            .body(DEPENDENCIES_PATH, Matchers.hasSize(0))
            .body(LINKS_PATH + ".keySet().size()", Matchers.is(2))
            .body(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY))
            .body(
                CLUSTER_COMMANDS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    CLUSTERS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS,
                    id
                )
            );

        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(1L);
    }

    @Test
    void canCreateClusterWithId() throws Exception {
        this.createConfigResource(
            new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(),
            null
        );

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(CLUSTERS_API + "/{id}", ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(ID))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(NAME))
            .body(USER_PATH, Matchers.is(USER))
            .body(VERSION_PATH, Matchers.is(VERSION))
            .body(TAGS_PATH, Matchers.hasItem("genie.id:" + ID))
            .body(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME))
            .body(SETUP_FILE_PATH, Matchers.nullValue())
            .body(STATUS_PATH, Matchers.is(ClusterStatus.UP.toString()))
            .body(CONFIGS_PATH, Matchers.hasSize(0))
            .body(DEPENDENCIES_PATH, Matchers.hasSize(0))
            .body(LINKS_PATH + ".keySet().size()", Matchers.is(2))
            .body(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY))
            .body(
                CLUSTER_COMMANDS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    CLUSTERS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS,
                    ID
                )
            );

        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(1L);
    }

    @Test
    void canHandleBadInputToCreateCluster() throws Exception {
        final Cluster cluster = new Cluster.Builder(" ", " ", " ", ClusterStatus.UP).build();

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(cluster))
            .when()
            .port(this.port)
            .post(CLUSTERS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()))
            .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
            .body(
                EXCEPTION_MESSAGE_PATH,
                Matchers.containsString("A version is required and must be at most 255 characters")
            );

        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(0L);
    }

    @Test
    void canFindClusters() throws Exception {
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
            new Cluster.Builder(name1, user1, version1, ClusterStatus.UP).withId(id1).build(),
            null
        );
        Thread.sleep(1000);
        this.createConfigResource(
            new Cluster.Builder(name2, user2, version2, ClusterStatus.OUT_OF_SERVICE).withId(id2).build(),
            null
        );
        Thread.sleep(1000);
        this.createConfigResource(
            new Cluster.Builder(name3, user3, version3, ClusterStatus.TERMINATED).withId(id3).build(),
            null
        );

        final RestDocumentationFilter findFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CLUSTER_SEARCH_QUERY_PARAMETERS, // Request query parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response headers
            Snippets.CLUSTER_SEARCH_RESULT_FIELDS, // Result fields
            Snippets.SEARCH_LINKS // HAL Links
        );

        // Test finding all clusters
        RestAssured
            .given(this.getRequestSpecification())
            .filter(findFilter)
            .when()
            .port(this.port)
            .get(CLUSTERS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(CLUSTERS_LIST_PATH, Matchers.hasSize(3))
            .body(CLUSTERS_ID_LIST_PATH, Matchers.containsInAnyOrder(id1, id2, id3))
            .body(
                CLUSTERS_COMMANDS_LINK_PATH,
                EntitiesLinksMatcher.matchUrisAnyOrder(
                    CLUSTERS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS,
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
            .get(CLUSTERS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(CLUSTERS_LIST_PATH, Matchers.hasSize(2));

        // Query by name
        RestAssured
            .given(this.getRequestSpecification())
            .filter(findFilter)
            .param("name", name2)
            .when()
            .port(this.port)
            .get(CLUSTERS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(CLUSTERS_LIST_PATH, Matchers.hasSize(1))
            .body(CLUSTERS_LIST_PATH + "[0].id", Matchers.is(id2));

        // Query by statuses
        RestAssured
            .given(this.getRequestSpecification())
            .filter(findFilter)
            .param("status", ClusterStatus.UP.toString(), ClusterStatus.TERMINATED.toString())
            .when()
            .port(this.port)
            .get(CLUSTERS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(CLUSTERS_LIST_PATH, Matchers.hasSize(2))
            .body(CLUSTERS_LIST_PATH + "[0].id", Matchers.is(id3))
            .body(CLUSTERS_LIST_PATH + "[1].id", Matchers.is(id1));

        // Query by tags
        RestAssured
            .given(this.getRequestSpecification())
            .filter(findFilter)
            .param("tag", "genie.id:" + id1)
            .when()
            .port(this.port)
            .get(CLUSTERS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(CLUSTERS_LIST_PATH, Matchers.hasSize(1))
            .body(CLUSTERS_LIST_PATH + "[0].id", Matchers.is(id1));

        //TODO: Add tests for searching by min and max update time as those are available parameters
        //TODO: Add tests for sort, orderBy etc

        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(3L);
    }

    @Test
    void canUpdateCluster() throws Exception {
        this.createConfigResource(
            new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(),
            null
        );
        final String clusterResource = CLUSTERS_API + "/{id}";
        final Cluster createdCluster = GenieObjectMapper.getMapper()
            .readValue(
                RestAssured
                    .given(this.getRequestSpecification())
                    .when()
                    .port(this.port)
                    .get(clusterResource, ID)
                    .then()
                    .statusCode(Matchers.is(HttpStatus.OK.value()))
                    .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
                    .extract()
                    .asByteArray(),
                new TypeReference<EntityModel<Cluster>>() {
                }
            ).getContent();
        Assertions.assertThat(createdCluster).isNotNull();
        Assertions.assertThat(createdCluster.getStatus()).isEqualByComparingTo(ClusterStatus.UP);

        final Cluster.Builder updateCluster = new Cluster.Builder(
            createdCluster.getName(),
            createdCluster.getUser(),
            createdCluster.getVersion(),
            ClusterStatus.OUT_OF_SERVICE
        )
            .withId(createdCluster.getId().orElseThrow(IllegalArgumentException::new))
            .withCreated(createdCluster.getCreated().orElseThrow(IllegalArgumentException::new))
            .withUpdated(createdCluster.getUpdated().orElseThrow(IllegalArgumentException::new))
            .withTags(createdCluster.getTags())
            .withConfigs(createdCluster.getConfigs())
            .withDependencies(createdCluster.getDependencies());

        createdCluster.getDescription().ifPresent(updateCluster::withDescription);
        createdCluster.getSetupFile().ifPresent(updateCluster::withSetupFile);

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // request header
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.getClusterRequestPayload() // payload fields
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(updateFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(updateCluster.build()))
            .when()
            .port(this.port)
            .put(clusterResource, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(clusterResource, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(STATUS_PATH, Matchers.is(ClusterStatus.OUT_OF_SERVICE.toString()));

        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(1L);
    }

    @Test
    void canPatchCluster() throws Exception {
        final String id = this.createConfigResource(
            new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(),
            null
        );
        final String clusterResource = CLUSTERS_API + "/{id}";

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(clusterResource, id)
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
            .patch(clusterResource, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(clusterResource, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(NAME_PATH, Matchers.is(newName));

        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(1L);
    }

    @Test
    void canDeleteAllClusters() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).build(), null);
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.OUT_OF_SERVICE).build(), null);
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.TERMINATED).build(), null);
        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(3L);

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/"
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(CLUSTERS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(0L);
    }

    @Test
    void canDeleteACluster() throws Exception {
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
            new Cluster.Builder(name1, user1, version1, ClusterStatus.UP).withId(id1).build(),
            null
        );
        this.createConfigResource(
            new Cluster.Builder(name2, user2, version2, ClusterStatus.OUT_OF_SERVICE).withId(id2).build(),
            null
        );
        this.createConfigResource(
            new Cluster.Builder(name3, user3, version3, ClusterStatus.TERMINATED).withId(id3).build(),
            null
        );
        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(3L);

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM // path parameters
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(CLUSTERS_API + "/{id}", id2)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(CLUSTERS_API + "/{id}", id2)
            .then()
            .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()))
            .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
            .body(
                EXCEPTION_MESSAGE_PATH,
                Matchers.containsString("No cluster with id")
            );

        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(2L);
    }

    @Test
    void canAddConfigsToCluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

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
        this.canAddElementsToResource(CLUSTERS_API + "/{id}/configs", ID, addFilter, getFilter);
    }

    @Test
    void canUpdateConfigsForCluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.CONFIG_FIELDS) // Request fields
        );
        this.canUpdateElementsForResource(CLUSTERS_API + "/{id}/configs", ID, updateFilter);
    }

    @Test
    void canDeleteConfigsForCluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM
        );
        this.canDeleteElementsFromResource(CLUSTERS_API + "/{id}/configs", ID, deleteFilter);
    }

    @Test
    void canAddDependenciesToCluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

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
        this.canAddElementsToResource(CLUSTERS_API + "/{id}/dependencies", ID, addFilter, getFilter);
    }

    @Test
    void canUpdateDependenciesForCluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.DEPENDENCIES_FIELDS) // Request fields
        );
        this.canUpdateElementsForResource(CLUSTERS_API + "/{id}/configs", ID, updateFilter);
    }

    @Test
    void canDeleteDependenciesForCluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM
        );
        this.canDeleteElementsFromResource(CLUSTERS_API + "/{id}/dependencies", ID, deleteFilter);
    }

    @Test
    void canAddTagsToCluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String api = CLUSTERS_API + "/{id}/tags";

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
    void canUpdateTagsForCluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String api = CLUSTERS_API + "/{id}/tags";

        final RestDocumentationFilter updateFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.TAGS_FIELDS) // Request fields
        );
        this.canUpdateTagsForResource(api, ID, NAME, updateFilter);
    }

    @Test
    void canDeleteTagsForCluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM
        );
        final String api = CLUSTERS_API + "/{id}/tags";
        this.canDeleteTagsForResource(api, ID, NAME, deleteFilter);
    }

    @Test
    void canDeleteTagForCluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String api = CLUSTERS_API + "/{id}/tags";
        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM.and(RequestDocumentation.parameterWithName("tag").description("The tag to remove"))
        );
        this.canDeleteTagForResource(api, ID, NAME, deleteFilter);
    }

    @Test
    void canAddCommandsForACluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String clusterCommandsAPI = CLUSTERS_API + "/{id}/commands";

        final RestDocumentationFilter addFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request Headers
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(
                PayloadDocumentation
                    .fieldWithPath("[]")
                    .description("Array of command ids (in preferred order) to append to the existing list of commands")
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
                    .writeValueAsBytes(Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            )
            .when()
            .port(this.port)
            .post(clusterCommandsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        // Test the filtering
        final RestDocumentationFilter getFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM, // Path parameters
            RequestDocumentation.requestParameters(
                RequestDocumentation
                    .parameterWithName("status")
                    .description("The status of commands to search for")
                    .attributes(Attributes.key(Snippets.CONSTRAINTS).value(CommandStatus.values()))
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
            .filter(getFilter)
            .param("status", CommandStatus.ACTIVE.toString())
            .when()
            .port(this.port)
            .get(clusterCommandsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.empty());
    }

    @Test
    void canSetCommandsForACluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String clusterCommandsAPI = CLUSTERS_API + "/{id}/commands";

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(clusterCommandsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.empty());

        final RestDocumentationFilter setFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.CONTENT_TYPE_HEADER, // Request Headers
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(
                PayloadDocumentation
                    .fieldWithPath("[]")
                    .description("Array of command ids (in preferred order) to replace the existing list of commands")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            ) // Request payload
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(setFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(
                GenieObjectMapper
                    .getMapper()
                    .writeValueAsBytes(Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            )
            .when()
            .port(this.port)
            .put(clusterCommandsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(clusterCommandsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.empty());
    }

    @Test
    void canRemoveCommandsFromACluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String clusterCommandsAPI = CLUSTERS_API + "/{id}/commands";

        final String placeholder = UUID.randomUUID().toString();
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, 7000L)
                .withId(commandId1)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, 8000L)
                .withId(commandId2)
                .build(),
            null
        );

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Lists.newArrayList(commandId1, commandId2)))
            .when()
            .port(this.port)
            .post(clusterCommandsAPI, ID)
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
            .delete(clusterCommandsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(clusterCommandsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.empty());
    }

    @Test
    void canRemoveCommandFromACluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String clusterCommandsAPI = CLUSTERS_API + "/{id}/commands";

        final RestDocumentationFilter deleteFilter = RestAssuredRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Snippets.ID_PATH_PARAM.and(
                RequestDocumentation.parameterWithName("commandId").description("The id of the command to remove")
            ) // Path parameters
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(clusterCommandsAPI + "/{commandId}", ID, UUID.randomUUID().toString())
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(clusterCommandsAPI, ID)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.empty());
    }

    /**
     * This test "documents" a known bug in Spring HATEOAS links that resulted in doubly-encoded pagination links.
     * https://github.com/spring-projects/spring-hateoas/issues/559
     * We worked around this bug in the UI by decoding these elements (see Pagination.js).
     * This test now documents the contract that this bug should be fixed.
     *
     * @throws Exception on error
     */
    @Test
    void testPagingDoubleEncoding() throws Exception {
        final String id1 = UUID.randomUUID().toString();
        final String id2 = UUID.randomUUID().toString();
        final String id3 = UUID.randomUUID().toString();
        final String name1 = "Test " + UUID.randomUUID().toString();
        final String name2 = "Test " + UUID.randomUUID().toString();
        final String name3 = "Test " + UUID.randomUUID().toString();
        final String user1 = UUID.randomUUID().toString();
        final String user2 = UUID.randomUUID().toString();
        final String user3 = UUID.randomUUID().toString();
        final String version1 = UUID.randomUUID().toString();
        final String version2 = UUID.randomUUID().toString();
        final String version3 = UUID.randomUUID().toString();

        this.createConfigResource(
            new Cluster.Builder(name1, user1, version1, ClusterStatus.UP).withId(id1).build(),
            null
        );
        Thread.sleep(1000);
        this.createConfigResource(
            new Cluster.Builder(name2, user2, version2, ClusterStatus.OUT_OF_SERVICE).withId(id2).build(),
            null
        );
        Thread.sleep(1000);
        this.createConfigResource(
            new Cluster.Builder(name3, user3, version3, ClusterStatus.TERMINATED).withId(id3).build(),
            null
        );

        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(3L);

        final URLEncoder urlEncoder = new URLEncoder();

        final String unencodedNameQuery = "Test %";
        final String singleEncodedNameQuery = urlEncoder.encode(unencodedNameQuery, StandardCharsets.UTF_8);

        // Query by name with wildcard and get the second page containing a single result (out of 3)
        final JsonNode responseJsonNode = GenieObjectMapper
            .getMapper()
            .readTree(
                RestAssured
                    .given(this.getRequestSpecification())
                    .param("name", unencodedNameQuery)
                    .param("size", 1)
                    .param("page", 1)
                    .when()
                    .port(this.port)
                    .get(CLUSTERS_API)
                    .then()
                    .statusCode(Matchers.is(HttpStatus.OK.value()))
                    .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
                    .body(CLUSTERS_LIST_PATH, Matchers.hasSize(1))
                    .extract()
                    .asByteArray()
            );

        // Self link is not double-encoded
        Assertions
            .assertThat(
                responseJsonNode
                    .get("_links")
                    .get("self")
                    .get("href")
                    .asText()
            )
            .contains(singleEncodedNameQuery);

        // Pagination links that were double-encoded
        final String[] doubleEncodedHREFS = new String[]{
            "first", "next", "prev", "last",
        };

        for (String doubleEncodedHref : doubleEncodedHREFS) {
            final String linkString = responseJsonNode
                .get("_links")
                .get(doubleEncodedHref)
                .get("href")
                .asText();
            Assertions.assertThat(linkString).isNotBlank();
            final Map<String, String> params = Maps.newHashMap();
            URLEncodedUtils
                .parse(new URI(linkString), StandardCharsets.UTF_8)
                .forEach(nameValuePair -> params.put(nameValuePair.getName(), nameValuePair.getValue()));

            Assertions.assertThat(params).containsKey("name");
            Assertions.assertThat(params.get("name")).isEqualTo(unencodedNameQuery);
        }
    }

    @Test
    void canCreateClusterWithBlankFields() throws Exception {
        final List<Cluster> invalidClusterResources = Lists.newArrayList(
            new Cluster
                .Builder(NAME, USER, VERSION, ClusterStatus.UP)
                .withId(UUID.randomUUID().toString())
                .withDependencies(Sets.newHashSet("foo", " "))
                .build(),

            new Cluster
                .Builder(NAME, USER, VERSION, ClusterStatus.UP)
                .withId(UUID.randomUUID().toString())
                .withConfigs(Sets.newHashSet("foo", " "))
                .build(),

            new Cluster
                .Builder(NAME, USER, VERSION, ClusterStatus.UP)
                .withId(UUID.randomUUID().toString())
                .withTags(Sets.newHashSet("foo", " "))
                .build()
        );

        long i = 0L;
        for (final Cluster invalidClusterResource : invalidClusterResources) {
            Assertions.assertThat(this.clusterRepository.count()).isEqualTo(i);

            RestAssured
                .given(this.getRequestSpecification())
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(GenieObjectMapper.getMapper().writeValueAsBytes(invalidClusterResource))
                .when()
                .port(this.port)
                .post(CLUSTERS_API)
                .then()
                .statusCode(Matchers.is(HttpStatus.CREATED.value()));

            Assertions.assertThat(this.clusterRepository.count()).isEqualTo(++i);
        }
    }

    @Test
    void testClusterNotFound() {
        final List<String> paths = Lists.newArrayList("", "/commands");

        for (final String path : paths) {
            RestAssured
                .given(this.getRequestSpecification())
                .when()
                .port(this.port)
                .get(CLUSTERS_API + "/{id}" + path, ID)
                .then()
                .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()))
                .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
                .body(EXCEPTION_MESSAGE_PATH, Matchers.startsWith("No cluster with id " + ID));
        }
    }
}
