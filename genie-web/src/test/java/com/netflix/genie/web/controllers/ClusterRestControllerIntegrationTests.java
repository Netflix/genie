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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaFileRepository;
import com.netflix.genie.core.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import org.apache.catalina.util.URLEncoder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.MediaType;
import org.springframework.restdocs.mockmvc.MockMvcRestDocumentation;
import org.springframework.restdocs.mockmvc.RestDocumentationRequestBuilders;
import org.springframework.restdocs.mockmvc.RestDocumentationResultHandler;
import org.springframework.restdocs.operation.preprocess.Preprocessors;
import org.springframework.restdocs.payload.PayloadDocumentation;
import org.springframework.restdocs.request.RequestDocumentation;
import org.springframework.restdocs.snippet.Attributes;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import javax.sql.DataSource;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

/**
 * Integration tests for the Commands REST API.
 *
 * @author tgianos
 * @since 3.0.0
 */
//TODO: Add tests for error conditions
public class ClusterRestControllerIntegrationTests extends RestControllerIntegrationTestsBase {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = "h2prod";
    private static final String USER = "genie";
    private static final String VERSION = "2.7.1";

    private static final String CLUSTERS_LIST_PATH = EMBEDDED_PATH + ".clusterList";
    private static final String CLUSTERS_ID_LIST_PATH = EMBEDDED_PATH + ".clusterList..id";
    private static final String CLUSTER_COMMANDS_LINK_PATH = "$._links.commands.href";
    private static final String CLUSTERS_COMMANDS_LINK_PATH = "$.._links.commands.href";

    @Autowired
    private JpaClusterRepository jpaClusterRepository;

    @Autowired
    private JpaCommandRepository jpaCommandRepository;

    @Autowired
    private JpaFileRepository fileRepository;

    @Autowired
    private JpaTagRepository tagRepository;

    @Autowired
    private DataSource dataSource;

    /**
     * Common setup for all tests.
     */
    @Before
    public void setup() {
        this.jpaClusterRepository.deleteAll();
        this.jpaCommandRepository.deleteAll();
        this.fileRepository.deleteAll();
        this.tagRepository.deleteAll();
    }

    /**
     * Cleanup after tests.
     */
    @After
    public void cleanup() {
        this.jpaClusterRepository.deleteAll();
        this.jpaCommandRepository.deleteAll();
        this.fileRepository.deleteAll();
        this.tagRepository.deleteAll();
    }

    /**
     * Test creating a cluster without an ID.
     *
     * @throws Exception on configuration issue
     */
    @Test
    public void canCreateClusterWithoutId() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));

        final RestDocumentationResultHandler creationResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request headers
            Snippets.getClusterRequestPayload(), // Request fields
            Snippets.LOCATION_HEADER // Response headers
        );

        final String id = this.createConfigResource(
            new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).build(),
            creationResultHandler
        );

        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // response headers
            Snippets.getClusterResponsePayload(), // response payload
            Snippets.CLUSTER_LINKS // response links
        );

        this.mvc
            .perform(RestDocumentationRequestBuilders.get(CLUSTERS_API + "/{id}", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(id)))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(SETUP_FILE_PATH, Matchers.nullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(ClusterStatus.UP.toString())))
            .andExpect(MockMvcResultMatchers.jsonPath(CONFIGS_PATH, Matchers.hasSize(0)))
            .andExpect(MockMvcResultMatchers.jsonPath(DEPENDENCIES_PATH, Matchers.hasSize(0)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH + ".*", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTER_COMMANDS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    CLUSTERS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS, id)))
            .andDo(getResultHandler);

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(1L));
    }

    /**
     * Test creating a Cluster with an ID.
     *
     * @throws Exception When issue in creation
     */
    @Test
    public void canCreateClusterWithId() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(),
            null
        );
        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API + "/" + ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(ID)))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.id:" + ID)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(SETUP_FILE_PATH, Matchers.nullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(ClusterStatus.UP.toString())))
            .andExpect(MockMvcResultMatchers.jsonPath(CONFIGS_PATH, Matchers.hasSize(0)))
            .andExpect(MockMvcResultMatchers.jsonPath(DEPENDENCIES_PATH, Matchers.hasSize(0)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH + ".*", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTER_COMMANDS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    CLUSTERS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS, ID)));
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure the post API can handle bad input.
     *
     * @throws Exception on issue
     */
    @Test
    public void canHandleBadInputToCreateCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        final Cluster cluster = new Cluster.Builder(null, null, null, null).build();
        this.mvc.perform(
            MockMvcRequestBuilders
                .post(CLUSTERS_API)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsBytes(cluster))
        ).andExpect(MockMvcResultMatchers.status().isPreconditionFailed());
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can search for clusters by various parameters.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canFindClusters() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
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

        final RestDocumentationResultHandler findResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CLUSTER_SEARCH_QUERY_PARAMETERS, // Request query parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response headers
            Snippets.CLUSTER_SEARCH_RESULT_FIELDS, // Result fields
            Snippets.SEARCH_LINKS // HAL Links
        );

        // Test finding all clusters
        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_ID_LIST_PATH, Matchers.containsInAnyOrder(
                id1, id2, id3)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_COMMANDS_LINK_PATH,
                EntitiesLinksMatcher.matchUrisAnyOrder(CLUSTERS_API, COMMANDS_LINK_KEY,
                    COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS, Lists.newArrayList(id1, id2, id3))))
            .andDo(findResultHandler);

        // Try to limit the number of results
        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API).param("size", "2"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(2)))
            .andDo(findResultHandler);

        // Query by name
        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API).param("name", name2))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH + "[0].id", Matchers.is(id2)))
            .andDo(findResultHandler);

        // Query by statuses
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .get(CLUSTERS_API)
                    .param("status", ClusterStatus.UP.toString(), ClusterStatus.TERMINATED.toString())
            )
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH + "[0].id", Matchers.is(id3)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH + "[1].id", Matchers.is(id1)))
            .andDo(findResultHandler);

        // Query by tags
        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API).param("tag", "genie.id:" + id1))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH + "[0].id", Matchers.is(id1)))
            .andDo(findResultHandler);

        //TODO: Add tests for searching by min and max update time as those are available parameters
        //TODO: Add tests for sort, orderBy etc

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(3L));
    }

    /**
     * Test to make sure that a cluster can be updated.
     *
     * @throws Exception on configuration errors
     */
    @Test
    public void canUpdateCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(),
            null
        );
        final String clusterResource = CLUSTERS_API + "/{id}";
        final Cluster createdCluster = this.objectMapper
            .readValue(
                this.mvc.perform(MockMvcRequestBuilders.get(clusterResource, ID))
                    .andReturn()
                    .getResponse()
                    .getContentAsByteArray(),
                ClusterResource.class
            ).getContent();
        Assert.assertThat(createdCluster.getStatus(), Matchers.is(ClusterStatus.UP));

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

        final RestDocumentationResultHandler updateResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // request header
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.getClusterRequestPayload() // payload fields
        );

        this.mvc.perform(
            RestDocumentationRequestBuilders
                .put(clusterResource, ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.objectMapper.writeValueAsBytes(updateCluster.build()))
        )
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(updateResultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterResource, ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(
                MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(ClusterStatus.OUT_OF_SERVICE.toString()))
            );
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure that a cluster can be patched.
     *
     * @throws Exception on configuration errors
     */
    @Test
    public void canPatchCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        final String id = this.createConfigResource(
            new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(),
            null
        );
        final String clusterResource = CLUSTERS_API + "/{id}";
        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterResource, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(NAME)));


        final String newName = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + newName + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(this.objectMapper.readTree(patchString));

        final RestDocumentationResultHandler patchResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // request headers
            Snippets.ID_PATH_PARAM, // path params
            Snippets.PATCH_FIELDS // request payload
        );

        this.mvc.perform(
            RestDocumentationRequestBuilders
                .patch(clusterResource, id)
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.objectMapper.writeValueAsBytes(patch))
        )
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(patchResultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterResource, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(newName)));
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(1L));
    }

    /**
     * Make sure can successfully delete all clusters.
     *
     * @throws Exception on a configuration error
     */
    @Test
    public void canDeleteAllClusters() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).build(), null);
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.OUT_OF_SERVICE).build(), null);
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.TERMINATED).build(), null);
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(3L));

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint())
        );

        this.mvc
            .perform(MockMvcRequestBuilders.delete(CLUSTERS_API))
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(deleteResultHandler);

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can delete a cluster.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canDeleteACluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
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
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(3L));

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM // path parameters
        );

        this.mvc
            .perform(RestDocumentationRequestBuilders.delete(CLUSTERS_API + "/{id}", id2))
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(deleteResultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API + "/{id}", id2))
            .andExpect(MockMvcResultMatchers.status().isNotFound());

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(2L));
    }

    /**
     * Test to make sure we can add configurations to the cluster after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddConfigsToCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationResultHandler addResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path params
            Snippets.CONTENT_TYPE_HEADER, // request header
            PayloadDocumentation.requestFields(Snippets.CONFIG_FIELDS) // response fields
        );
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path params
            Snippets.JSON_CONTENT_TYPE_HEADER, // response headers
            PayloadDocumentation.responseFields(Snippets.CONFIG_FIELDS) // response fields
        );
        this.canAddElementsToResource(CLUSTERS_API + "/{id}/configs", ID, addResultHandler, getResultHandler);
    }

    /**
     * Test to make sure we can update the configurations for a cluster after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateConfigsForCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationResultHandler updateResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.CONFIG_FIELDS) // Request fields
        );
        this.canUpdateElementsForResource(CLUSTERS_API + "/{id}/configs", ID, updateResultHandler);
    }

    /**
     * Test to make sure we can delete the configurations for a cluster after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteConfigsForCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM
        );
        this.canDeleteElementsFromResource(CLUSTERS_API + "/{id}/configs", ID, deleteResultHandler);
    }

    /**
     * Test to make sure we can add dependencies to the cluster after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddDependenciesToCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationResultHandler addResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path params
            Snippets.CONTENT_TYPE_HEADER, // request header
            PayloadDocumentation.requestFields(Snippets.DEPENDENCIES_FIELDS) // response fields
        );
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path params
            Snippets.JSON_CONTENT_TYPE_HEADER, // response headers
            PayloadDocumentation.responseFields(Snippets.DEPENDENCIES_FIELDS) // response fields
        );
        this.canAddElementsToResource(CLUSTERS_API + "/{id}/dependencies", ID, addResultHandler, getResultHandler);
    }

    /**
     * Test to make sure we can update the dependencies for a cluster after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateDependenciesForCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationResultHandler updateResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.DEPENDENCIES_FIELDS) // Request fields
        );
        this.canUpdateElementsForResource(CLUSTERS_API + "/{id}/configs", ID, updateResultHandler);
    }

    /**
     * Test to make sure we can delete the dependencies for a cluster after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteDependenciesForCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM
        );
        this.canDeleteElementsFromResource(CLUSTERS_API + "/{id}/dependencies", ID, deleteResultHandler);
    }

    /**
     * Test to make sure we can add tags to the cluster after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddTagsToCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String api = CLUSTERS_API + "/{id}/tags";

        final RestDocumentationResultHandler addResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.TAGS_FIELDS) // Request fields
        );
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.JSON_CONTENT_TYPE_HEADER, // Response header
            PayloadDocumentation.responseFields(Snippets.TAGS_FIELDS)
        );
        this.canAddTagsToResource(api, ID, NAME, addResultHandler, getResultHandler);
    }

    /**
     * Test to make sure we can update the tags for a cluster after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateTagsForCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String api = CLUSTERS_API + "/{id}/tags";

        final RestDocumentationResultHandler updateResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.TAGS_FIELDS) // Request fields
        );
        this.canUpdateTagsForResource(api, ID, NAME, updateResultHandler);
    }

    /**
     * Test to make sure we can delete the tags for a cluster after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteTagsForCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM
        );
        final String api = CLUSTERS_API + "/{id}/tags";
        this.canDeleteTagsForResource(api, ID, NAME, deleteResultHandler);
    }

    /**
     * Test to make sure we can delete a tag for a cluster after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteTagForCluster() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String api = CLUSTERS_API + "/{id}/tags";
        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM.and(
                RequestDocumentation.parameterWithName("tag").description("The tag to remove")
            )
        );
        this.canDeleteTagForResource(api, ID, NAME, deleteResultHandler);
    }

    /**
     * Make sure can add the commands for a cluster.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canAddCommandsForACluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String clusterCommandsAPI = CLUSTERS_API + "/{id}/commands";
        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI, ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));

        final String placeholder = UUID.randomUUID().toString();
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder, 1000L)
                .withId(commandId1)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder, 2000L)
                .withId(commandId2)
                .build(),
            null
        );

        final RestDocumentationResultHandler addResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request Headers
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(
                PayloadDocumentation
                    .fieldWithPath("[]")
                    .description("Array of command ids (in preferred order) to append to the existing list of commands")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            ) // Request payload
        );

        this.mvc
            .perform(
                RestDocumentationRequestBuilders
                    .post(clusterCommandsAPI, ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(Lists.newArrayList(commandId1, commandId2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(addResultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI, ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(commandId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(commandId2)));

        //Shouldn't add anything
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(clusterCommandsAPI, ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(Lists.newArrayList()))
            )
            .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());

        final String commandId3 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.INACTIVE, placeholder, 1000L)
                .withId(commandId3)
                .build(),
            null
        );
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(clusterCommandsAPI, ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(Lists.newArrayList(commandId3)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI, ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(commandId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(commandId2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[2].id", Matchers.is(commandId3)));

        // Test the filtering
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
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
                    .fieldWithPath("[]")
                    .description("The list of commands found")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            )
        );
        this.mvc
            .perform(
                RestDocumentationRequestBuilders
                    .get(clusterCommandsAPI, ID).param("status", CommandStatus.INACTIVE.toString())
            )
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(commandId3)))
            .andDo(getResultHandler);
    }

    /**
     * Make sure can set the commands for a cluster.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canSetCommandsForACluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String clusterCommandsAPI = CLUSTERS_API + "/{id}/commands";
        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI, ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));

        final String placeholder = UUID.randomUUID().toString();
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder, 4000L)
                .withId(commandId1)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder, 5000L)
                .withId(commandId2)
                .build(),
            null
        );

        final RestDocumentationResultHandler setResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request Headers
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(
                PayloadDocumentation
                    .fieldWithPath("[]")
                    .description("Array of command ids (in preferred order) to replace the existing list of commands")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            ) // Request payload
        );

        this.mvc
            .perform(
                RestDocumentationRequestBuilders
                    .put(clusterCommandsAPI, ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(Lists.newArrayList(commandId1, commandId2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(setResultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI, ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(commandId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(commandId2)));

        //Should clear commands
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .put(clusterCommandsAPI, ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(Lists.newArrayList()))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI, ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));
    }

    /**
     * Make sure that we can remove all the commands from a cluster.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canRemoveCommandsFromACluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String clusterCommandsAPI = CLUSTERS_API + "/{id}/commands";

        final String placeholder = UUID.randomUUID().toString();
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder, 7000L)
                .withId(commandId1)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder, 8000L)
                .withId(commandId2)
                .build(),
            null
        );

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(clusterCommandsAPI, ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(commandId1, commandId2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM // Path parameters
        );

        this.mvc
            .perform(RestDocumentationRequestBuilders.delete(clusterCommandsAPI, ID))
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(deleteResultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI, ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));
    }

    /**
     * Make sure that we can remove a command from a cluster.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canRemoveCommandFromACluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String clusterCommandsAPI = CLUSTERS_API + "/{id}/commands";

        final String placeholder = UUID.randomUUID().toString();
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        final String commandId3 = UUID.randomUUID().toString();
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder, 1000L)
                .withId(commandId1)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder, 2000L)
                .withId(commandId2)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder, 3000L)
                .withId(commandId3)
                .build(),
            null
        );

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(clusterCommandsAPI, ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        this.objectMapper.writeValueAsBytes(Lists.newArrayList(commandId1, commandId2, commandId3))
                    )
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM.and(
                RequestDocumentation.parameterWithName("commandId").description("The id of the command to remove")
            ) // Path parameters
        );

        this.mvc
            .perform(RestDocumentationRequestBuilders.delete(clusterCommandsAPI + "/{commandId}", ID, commandId2))
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(deleteResultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI, ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(commandId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(commandId3)));

        // Check reverse side of relationship
        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API + "/{id}/clusters", commandId1))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(ID)));

        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API + "/{id}/clusters", commandId2))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));

        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API + "/{id}/clusters", commandId3))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(ID)));
    }

    /**
     * This test "documents" a known bug in Spring HATEOAS links that results in doubly-encoded pagination links.
     * https://github.com/spring-projects/spring-hateoas/issues/559
     * Currently, we work around this bug in the UI by decoding these elements (see Pagination.js).
     * If this test starts failing, it may be because the behavior has been corrected, and the workaround may be
     * removed.
     *
     * @throws Exception on error
     */
    @Test
    public void testPagingDoubleEncoding() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
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

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(3L));

        final URLEncoder urlEncoder = new URLEncoder();

        final String unencodedNameQuery = "Test %";
        final String singleEncodedNameQuery = urlEncoder.encode(unencodedNameQuery, StandardCharsets.UTF_8);
        final String doubleEncodedNameQuery = urlEncoder.encode(singleEncodedNameQuery, StandardCharsets.UTF_8);

        // Query by name with wildcard and get the second page containing a single result (out of 3)
        final MvcResult response = this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API)
                .param("name", unencodedNameQuery)
                .param("size", "1")
                .param("page", "1")
            )
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(1)))
            .andReturn();

        final JsonNode responseJsonNode = new ObjectMapper().readTree(response.getResponse().getContentAsString());

        // Self link is not double-encoded
        Assert.assertTrue(
            responseJsonNode
                .get("_links")
                .get("self")
                .get("href")
                .asText()
                .contains(singleEncodedNameQuery));

        // Pagination links are double-encoded

        final String[] doubleEncodedHrefs = new String[] {
            "first", "next", "prev", "last",
        };

        for (String doubleEncodedHref : doubleEncodedHrefs) {
            final String linkString = responseJsonNode.get("_links").get(doubleEncodedHref).get("href").asText();
            Assert.assertNotNull(linkString);
            final HashMap<String, String> params = Maps.newHashMap();
            URLEncodedUtils.parse(new URI(linkString), StandardCharsets.UTF_8)
                .forEach(nameValuePair -> params.put(nameValuePair.getName(), nameValuePair.getValue()));

            Assert.assertTrue(params.containsKey("name"));
            // Correct: singleEncodedNameQuery, actual: doubleEncodedNameQuery
            Assert.assertEquals(doubleEncodedNameQuery, params.get("name"));
            final String decoded = URLDecoder.decode(params.get("name"), StandardCharsets.UTF_8.name());
            Assert.assertEquals(singleEncodedNameQuery, decoded);
        }
    }

    /**
     * Test creating a Cluster with a blank setup file path.
     *
     * @throws Exception When issue in creation
     */
    @Test
    public void cantCreateClusterWithBlankSetupFile() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));

        final Cluster clusterResource = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP)
            .withId(ID)
            .withSetupFile(" ")
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders.post(CLUSTERS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(clusterResource))
            )
            .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
    }

    /**
     * Test creating a Cluster with a blank config file path.
     *
     * @throws Exception When issue in creation
     */
    @Test
    public void cantCreateClusterWithBlankConfigFile() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));

        final Cluster clusterResource = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP)
            .withId(ID)
            .withConfigs(Sets.newHashSet("foo", " "))
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders.post(CLUSTERS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(clusterResource))
            )
            .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
    }

    /**
     * Test creating a Cluster with a blank tag.
     *
     * @throws Exception When issue in creation
     */
    @Test
    public void cantCreateClusterWithBlankTag() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));

        final Cluster clusterResource = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP)
            .withId(ID)
            .withTags(Sets.newHashSet("foo", " "))
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders.post(CLUSTERS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(clusterResource))
            )
            .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
    }
}
