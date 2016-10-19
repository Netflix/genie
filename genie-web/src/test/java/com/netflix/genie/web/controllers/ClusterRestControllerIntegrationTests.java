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
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.aspect.DataServiceRetryAspect;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.MediaType;
import org.springframework.retry.RetryListener;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
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

    @Autowired
    private JpaClusterRepository jpaClusterRepository;

    @Autowired
    private JpaCommandRepository jpaCommandRepository;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private DataServiceRetryAspect dataServiceRetryAspect;

    /**
     * Cleanup after tests.
     */
    @After
    public void cleanup() {
        this.jpaClusterRepository.deleteAll();
        this.jpaCommandRepository.deleteAll();
    }

    /**
     * Test creating a cluster without an ID.
     *
     * @throws Exception on configuration issue
     */
    @Test
    public void canCreateClusterWithoutId() throws Exception {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        final String id = this.createConfigResource(
            new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).build(),
            null
        );
        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API + "/" + id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
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
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH + ".*", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY)));
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
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
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
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH + ".*", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY)));
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

        // Test finding all clusters
        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(3)));

        // Try to limit the number of results
        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API).param("size", "2"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(2)));

        // Query by name
        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API).param("name", name2))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH + "[0].id", Matchers.is(id2)));

        // Query by statuses
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .get(CLUSTERS_API)
                    .param("status", ClusterStatus.UP.toString(), ClusterStatus.TERMINATED.toString())
            )
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH + "[0].id", Matchers.is(id3)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH + "[1].id", Matchers.is(id1)));

        // Query by tags
        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API).param("tag", "genie.id:" + id1))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTERS_LIST_PATH + "[0].id", Matchers.is(id1)));

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
        final String clusterResource = CLUSTERS_API + "/" + ID;
        final Cluster createdCluster = this.objectMapper
            .readValue(
                this.mvc.perform(
                    MockMvcRequestBuilders.get(clusterResource)
                )
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
            .withConfigs(createdCluster.getConfigs());

        createdCluster.getDescription().ifPresent(updateCluster::withDescription);
        createdCluster.getSetupFile().ifPresent(updateCluster::withSetupFile);

        this.mvc.perform(
            MockMvcRequestBuilders
                .put(clusterResource)
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.objectMapper.writeValueAsBytes(updateCluster.build()))
        ).andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterResource))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
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
        final String clusterResource = CLUSTERS_API + "/" + id;
        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterResource))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(NAME)));


        final String newName = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + newName + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(objectMapper.readTree(patchString));

        this.mvc.perform(
            MockMvcRequestBuilders
                .patch(clusterResource)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsBytes(patch))
        ).andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterResource))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
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

        this.mvc
            .perform(MockMvcRequestBuilders.delete(CLUSTERS_API))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
    }

    /**
     * Make sure can successfully delete all clusters with retries.
     *
     * @throws Exception on a configuration error
     */
    @Test
    public void canDeleteAllClustersWithRetry() throws Exception {
        final RetryListener retryListener = Mockito.mock(RetryListener.class);
        final Connection conn = dataSource.getConnection();
        final Statement stmt = conn.createStatement();
        try {
            Mockito.when(retryListener.open(Mockito.any(), Mockito.any())).thenReturn(true);
            final RetryListener[] retryListeners = {retryListener};
            dataServiceRetryAspect.setRetryListeners(retryListeners);
            Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
            this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).build(), null);
            conn.setAutoCommit(false);
            stmt.execute("LOCK TABLE clusters WRITE");
            this.mvc
                .perform(MockMvcRequestBuilders.delete(CLUSTERS_API))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
            Mockito.verify(retryListener, Mockito.times(2)).onError(Mockito.any(), Mockito.any(), Mockito.any());
            Mockito.doAnswer(invocation -> {
                conn.commit();
                return null;
            }).when(retryListener).onError(Mockito.any(), Mockito.any(), Mockito.any());
            this.mvc
                .perform(MockMvcRequestBuilders.delete(CLUSTERS_API))
                .andExpect(MockMvcResultMatchers.status().isNoContent());
            Mockito.verify(retryListener, Mockito.times(3)).onError(Mockito.any(), Mockito.any(), Mockito.any());
            Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        } finally {
            stmt.close();
            conn.commit();
            conn.close();
            dataServiceRetryAspect.setRetryListeners(new RetryListener[0]);
        }
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

        this.mvc
            .perform(MockMvcRequestBuilders.delete(CLUSTERS_API + "/" + id2))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(CLUSTERS_API + "/" + id2))
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
        this.canAddElementsToResource(CLUSTERS_API + "/" + ID + "/configs");
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
        this.canUpdateElementsForResource(CLUSTERS_API + "/" + ID + "/configs");
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
        this.canDeleteElementsFromResource(CLUSTERS_API + "/" + ID + "/configs");
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
        final String api = CLUSTERS_API + "/" + ID + "/tags";
        this.canAddTagsToResource(api, ID, NAME);
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
        final String api = CLUSTERS_API + "/" + ID + "/tags";
        this.canUpdateTagsForResource(api, ID, NAME);
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
        final String api = CLUSTERS_API + "/" + ID + "/tags";
        this.canDeleteTagsForResource(api, ID, NAME);
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
        final String api = CLUSTERS_API + "/" + ID + "/tags";
        this.canDeleteTagForResource(api, ID, NAME);
    }

    /**
     * Make sure can add the commands for a cluster.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canAddCommandsForACluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String clusterCommandsAPI = CLUSTERS_API + "/" + ID + "/commands";
        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
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

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(clusterCommandsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(commandId1, commandId2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(commandId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(commandId2)));

        //Shouldn't add anything
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(clusterCommandsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList()))
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
                    .post(clusterCommandsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(commandId3)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(commandId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(commandId2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[2].id", Matchers.is(commandId3)));

        // Test the filtering

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI).param("status", CommandStatus.INACTIVE.toString()))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(commandId3)));
    }

    /**
     * Make sure can set the commands for a cluster.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canSetCommandsForACluster() throws Exception {
        this.createConfigResource(new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build(), null);
        final String clusterCommandsAPI = CLUSTERS_API + "/" + ID + "/commands";
        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
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

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .put(clusterCommandsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(commandId1, commandId2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(commandId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(commandId2)));

        //Should clear commands
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .put(clusterCommandsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList()))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
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
        final String clusterCommandsAPI = CLUSTERS_API + "/" + ID + "/commands";

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
                    .post(clusterCommandsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(commandId1, commandId2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.delete(clusterCommandsAPI))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
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
        final String clusterCommandsAPI = CLUSTERS_API + "/" + ID + "/commands";

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
                    .post(clusterCommandsAPI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Lists.newArrayList(commandId1, commandId2, commandId3)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.delete(clusterCommandsAPI + "/" + commandId2))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(clusterCommandsAPI))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(commandId1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(commandId3)));

        // Check reverse side of relationship
        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API + "/" + commandId1 + "/clusters"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(ID)));

        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API + "/" + commandId2 + "/clusters"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));

        this.mvc
            .perform(MockMvcRequestBuilders.get(COMMANDS_API + "/" + commandId3 + "/clusters"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(ID)));
    }
}
