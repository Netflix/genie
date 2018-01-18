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
package com.netflix.genie.web.jpa.services;

import com.github.fge.jsonpatch.JsonPatch;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.services.ClusterService;
import com.netflix.genie.web.services.CommandService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.validation.ConstraintViolationException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Instant;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the CommandServiceJPAImpl.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(IntegrationTest.class)
@DatabaseSetup("JpaClusterServiceImplIntegrationTests/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaClusterServiceImplIntegrationTests extends DBUnitTestBase {

    private static final String COMMAND_1_ID = "command1";
    private static final String COMMAND_2_ID = "command2";
    private static final String COMMAND_3_ID = "command3";

    private static final String CLUSTER_1_ID = "cluster1";
    private static final String CLUSTER_1_USER = "tgianos";
    private static final String CLUSTER_1_NAME = "h2prod";
    private static final String CLUSTER_1_VERSION = "2.4.0";
    private static final ClusterStatus CLUSTER_1_STATUS = ClusterStatus.UP;

    private static final String CLUSTER_2_ID = "cluster2";
    private static final String CLUSTER_2_USER = "amsharma";
    private static final String CLUSTER_2_NAME = "h2query";
    private static final String CLUSTER_2_VERSION = "2.4.0";
    private static final ClusterStatus CLUSTER_2_STATUS = ClusterStatus.UP;

    private static final Pageable PAGE = new PageRequest(0, 10, Sort.Direction.DESC, "updated");

    @Autowired
    private ClusterService service;

    @Autowired
    private CommandService commandService;

    @Autowired
    private JpaClusterRepository clusterRepository;

    /**
     * Test the get cluster method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetCluster() throws GenieException {
        final Cluster cluster1 = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_1_ID, cluster1.getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(CLUSTER_1_NAME, cluster1.getName());
        Assert.assertEquals(CLUSTER_1_USER, cluster1.getUser());
        Assert.assertEquals(CLUSTER_1_VERSION, cluster1.getVersion());
        Assert.assertEquals(CLUSTER_1_STATUS, cluster1.getStatus());
        Assert.assertEquals(5, cluster1.getTags().size());
        Assert.assertEquals(1, cluster1.getConfigs().size());
        Assert.assertEquals(2, cluster1.getDependencies().size());

        final Cluster cluster2 = this.service.getCluster(CLUSTER_2_ID);
        Assert.assertEquals(CLUSTER_2_ID, cluster2.getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(CLUSTER_2_NAME, cluster2.getName());
        Assert.assertEquals(CLUSTER_2_USER, cluster2.getUser());
        Assert.assertEquals(CLUSTER_2_VERSION, cluster2.getVersion());
        Assert.assertEquals(CLUSTER_2_STATUS, cluster2.getStatus());
        Assert.assertEquals(5, cluster2.getTags().size());
        Assert.assertEquals(2, cluster2.getConfigs().size());
        Assert.assertEquals(0, cluster2.getDependencies().size());
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByName() {
        final Page<Cluster> clusters = this.service.getClusters(CLUSTER_2_NAME, null, null, null, null, PAGE);
        Assert.assertEquals(1, clusters.getNumberOfElements());
        Assert.assertEquals(
            CLUSTER_2_ID, clusters.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByStatuses() {
        final Set<ClusterStatus> statuses = EnumSet.noneOf(ClusterStatus.class);
        statuses.add(ClusterStatus.UP);
        final Page<Cluster> clusters = this.service.getClusters(null, statuses, null, null, null, PAGE);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(
            CLUSTER_2_ID, clusters.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            CLUSTER_1_ID, clusters.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByTags() {
        final Set<String> tags = Sets.newHashSet("prod");
        Page<Cluster> clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assert.assertEquals(1, clusters.getNumberOfElements());
        Assert.assertEquals(
            CLUSTER_1_ID, clusters.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );

        tags.clear();
        tags.add("hive");
        clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(
            CLUSTER_2_ID, clusters.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            CLUSTER_1_ID, clusters.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );

        tags.add("somethingThatWouldNeverReallyExist");
        clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assert.assertTrue(clusters.getContent().isEmpty());

        tags.clear();
        clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(
            CLUSTER_2_ID, clusters.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            CLUSTER_1_ID, clusters.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByMinUpdateTime() {
        final ZonedDateTime time = ZonedDateTime.of(2014, Month.JULY.getValue(), 9, 2, 58, 59, 0, ZoneId.of("UTC"));
        final Page<Cluster> clusters
            = this.service.getClusters(null, null, null, time.toInstant(), null, PAGE);
        Assert.assertEquals(1, clusters.getNumberOfElements());
        Assert.assertEquals(
            CLUSTER_2_ID, clusters.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByMaxUpdateTime() {
        final ZonedDateTime time = ZonedDateTime.of(2014, Month.JULY.getValue(), 8, 3, 0, 0, 0, ZoneId.of("UTC"));
        final Page<Cluster> clusters
            = this.service.getClusters(null, null, null, null, time.toInstant(), PAGE);
        Assert.assertEquals(1, clusters.getNumberOfElements());
        Assert.assertEquals(
            CLUSTER_1_ID, clusters.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get clusters method with descending sort.
     */
    @Test
    public void testGetClustersDescending() {
        //Default to order by Updated
        final Page<Cluster> clusters = this.service.getClusters(null, null, null, null, null, PAGE);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(
            CLUSTER_2_ID, clusters.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            CLUSTER_1_ID, clusters.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get clusters method with ascending sort.
     */
    @Test
    public void testGetClustersAscending() {
        final Pageable ascendingPage = new PageRequest(0, 10, Sort.Direction.ASC, "updated");
        //Default to order by Updated
        final Page<Cluster> clusters = this.service.getClusters(null, null, null, null, null, ascendingPage);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(
            CLUSTER_1_ID, clusters.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            CLUSTER_2_ID, clusters.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get clusters method order by name.
     */
    @Test
    public void testGetClustersOrderBysUser() {
        final Pageable userPage = new PageRequest(0, 10, Sort.Direction.DESC, "user");
        final Page<Cluster> clusters = this.service.getClusters(null, null, null, null, null, userPage);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(
            CLUSTER_1_ID, clusters.getContent().get(0).getId().orElseThrow(IllegalArgumentException::new)
        );
        Assert.assertEquals(
            CLUSTER_2_ID, clusters.getContent().get(1).getId().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Test the get clusters method order by an invalid field should return the order by default value (updated).
     */
    @Test(expected = RuntimeException.class)
    public void testGetClustersOrderBysInvalidField() {
        final Pageable badPage = new PageRequest(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
        this.service.getClusters(null, null, null, null, null, badPage);
    }

    /**
     * Test the choseClusterForJobRequest function.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testChooseClusterForJob() throws GenieException {
        final JobRequest one = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("genie.id:cluster1"))),
            Sets.newHashSet("pig")
        ).build();
        final JobRequest two = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("genie.id:cluster"))),
            Sets.newHashSet("pig")
        ).build();
        final JobRequest three = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("genie.id:cluster1"))),
            Sets.newHashSet("pi")
        ).build();
        final JobRequest four = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("pig"))),
            Sets.newHashSet("pig")
        ).build();
        final JobRequest five = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("pig", "hive"))),
            Sets.newHashSet("pig")
        ).build();

        Assert.assertThat(this.service.findClustersAndCommandsForJob(one).size(), Matchers.is(1));
        Assert.assertThat(this.service.findClustersAndCommandsForJob(two).size(), Matchers.is(0));
        Assert.assertThat(this.service.findClustersAndCommandsForJob(three).size(), Matchers.is(0));
        Assert.assertThat(this.service.findClustersAndCommandsForJob(four).size(), Matchers.is(2));
        Assert.assertThat(this.service.findClustersAndCommandsForJob(five).size(), Matchers.is(2));
    }

    /**
     * Test the create method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateCluster() throws GenieException {
        final Set<String> configs = Sets.newHashSet("a config", "another config", "yet another config");
        final Set<String> dependencies = Sets.newHashSet("a dependency");
        final String id = UUID.randomUUID().toString();
        final Cluster cluster = new Cluster.Builder(
            CLUSTER_1_NAME,
            CLUSTER_1_USER,
            CLUSTER_1_VERSION,
            ClusterStatus.OUT_OF_SERVICE
        )
            .withId(id)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .build();
        this.service.createCluster(cluster);
        final Cluster created = this.service.getCluster(id);
        Assert.assertNotNull(created);
        Assert.assertEquals(id, created.getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(CLUSTER_1_NAME, created.getName());
        Assert.assertEquals(CLUSTER_1_USER, created.getUser());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, created.getStatus());
        Assert.assertEquals(3, created.getConfigs().size());
        Assert.assertEquals(1, created.getDependencies().size());
        this.service.deleteCluster(id);
        try {
            this.service.getCluster(id);
            Assert.fail("Should have thrown exception");
        } catch (final GenieException ge) {
            Assert.assertEquals(
                HttpURLConnection.HTTP_NOT_FOUND,
                ge.getErrorCode()
            );
        }
    }

    /**
     * Test the create method when no id is entered.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateClusterNoId() throws GenieException {
        final Set<String> configs = Sets.newHashSet("a config", "another config", "yet another config");
        final Set<String> dependencies = Sets.newHashSet("a dependency");
        final Cluster cluster = new Cluster.Builder(
            CLUSTER_1_NAME,
            CLUSTER_1_USER,
            CLUSTER_1_VERSION,
            ClusterStatus.OUT_OF_SERVICE
        )
            .withConfigs(configs)
            .withDependencies(dependencies)
            .build();
        final String id = this.service.createCluster(cluster);
        final Cluster created = this.service.getCluster(id);
        Assert.assertEquals(CLUSTER_1_NAME, created.getName());
        Assert.assertEquals(CLUSTER_1_USER, created.getUser());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, created.getStatus());
        Assert.assertEquals(3, created.getConfigs().size());
        Assert.assertEquals(1, created.getDependencies().size());
        this.service.deleteCluster(created.getId().orElseThrow(IllegalArgumentException::new));
        try {
            this.service.getCluster(created.getId().orElseThrow(IllegalArgumentException::new));
            Assert.fail("Should have thrown exception");
        } catch (final GenieException ge) {
            Assert.assertEquals(
                HttpURLConnection.HTTP_NOT_FOUND,
                ge.getErrorCode()
            );
        }
    }

    /**
     * Test to update a cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateClusterNoId() throws GenieException {
        final Cluster getCluster = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_1_USER, getCluster.getUser());
        Assert.assertEquals(ClusterStatus.UP, getCluster.getStatus());
        Assert.assertEquals(5, getCluster.getTags().size());

        final Set<String> tags = Sets.newHashSet("tez", "yarn", "hadoop");
        tags.addAll(getCluster.getTags());

        final Cluster.Builder updateCluster = new Cluster.Builder(
            getCluster.getName(),
            CLUSTER_2_USER,
            getCluster.getVersion(),
            ClusterStatus.OUT_OF_SERVICE
        )
            .withId(getCluster.getId().orElseThrow(IllegalArgumentException::new))
            .withCreated(getCluster.getCreated().orElseThrow(IllegalArgumentException::new))
            .withUpdated(getCluster.getUpdated().orElseThrow(IllegalArgumentException::new))
            .withTags(tags)
            .withConfigs(getCluster.getConfigs());

        getCluster.getDescription().ifPresent(updateCluster::withDescription);
        getCluster.getSetupFile().ifPresent(updateCluster::withSetupFile);

        this.service.updateCluster(CLUSTER_1_ID, updateCluster.build());

        final Cluster updated = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_2_USER, updated.getUser());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, updated.getStatus());
        Assert.assertEquals(8, updated.getTags().size());
    }

    /**
     * Test to update a cluster with invalid content. Should throw ConstraintViolationException from JPA layer.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateClusterWithInvalidCluster() throws GenieException {
        final Cluster getCluster = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_1_USER, getCluster.getUser());
        Assert.assertEquals(ClusterStatus.UP, getCluster.getStatus());
        Assert.assertEquals(5, getCluster.getTags().size());

        final Cluster updateCluster = new Cluster.Builder(
            "", //invalid
            getCluster.getUser(),
            getCluster.getVersion(),
            ClusterStatus.OUT_OF_SERVICE
        )
            .withId(getCluster.getId().orElseThrow(IllegalArgumentException::new))
            .build();

        this.service.updateCluster(CLUSTER_1_ID, updateCluster);
    }

    /**
     * Test to make sure setting the created and updated outside the system control doesn't change record in database.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateCreateAndUpdate() throws GenieException {
        final Cluster getCluster = this.service.getCluster(CLUSTER_1_ID);
        final Instant created = getCluster.getCreated().orElseThrow(IllegalArgumentException::new);
        final Instant updated = getCluster.getUpdated().orElseThrow(IllegalArgumentException::new);

        final Cluster.Builder updateCluster = new Cluster.Builder(
            getCluster.getName(),
            getCluster.getUser(),
            getCluster.getVersion(),
            getCluster.getStatus()
        )
            .withId(getCluster.getId().orElseThrow(IllegalArgumentException::new))
            .withCreated(Instant.now())
            .withUpdated(Instant.EPOCH)
            .withTags(getCluster.getTags())
            .withConfigs(getCluster.getConfigs())
            .withDependencies(getCluster.getDependencies());

        getCluster.getDescription().ifPresent(updateCluster::withDescription);
        getCluster.getSetupFile().ifPresent(updateCluster::withSetupFile);

        this.service.updateCluster(CLUSTER_1_ID, updateCluster.build());
        final Cluster updatedCluster = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(created, updatedCluster.getCreated().orElseThrow(IllegalArgumentException::new));
        Assert.assertNotEquals(updated, updatedCluster.getUpdated());
        Assert.assertNotEquals(Instant.EPOCH, updatedCluster.getUpdated().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(getCluster.getTags(), updatedCluster.getTags());
        Assert.assertEquals(getCluster.getConfigs(), updatedCluster.getConfigs());
        Assert.assertEquals(getCluster.getDependencies(), updatedCluster.getDependencies());
    }

    /**
     * Test to patch a cluster.
     *
     * @throws GenieException For any problem
     * @throws IOException    For Json serialization problem
     */
    @Test
    public void testPatchCluster() throws GenieException, IOException {
        final Cluster getCluster = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertThat(getCluster.getName(), Matchers.is(CLUSTER_1_NAME));
        final Instant updateTime = getCluster.getUpdated().orElseThrow(IllegalArgumentException::new);

        final String patchString
            = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + CLUSTER_2_NAME + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));

        this.service.patchCluster(CLUSTER_1_ID, patch);

        final Cluster updated = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertNotEquals(updated.getUpdated(), Matchers.is(updateTime));
        Assert.assertThat(updated.getName(), Matchers.is(CLUSTER_2_NAME));
    }

    /**
     * Test delete all.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDeleteAll() throws GenieException {
        Assert.assertEquals(2, this.service.getClusters(null, null, null, null, null, PAGE).getNumberOfElements());
        this.service.deleteAllClusters();
        Assert.assertTrue(this.service.getClusters(null, null, null, null, null, PAGE).getContent().isEmpty());
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDelete() throws GenieException {
        Assert.assertEquals(2, this.commandService.getClustersForCommand(COMMAND_1_ID, null).size());
        Assert.assertEquals(2, this.commandService.getClustersForCommand(COMMAND_2_ID, null).size());
        Assert.assertEquals(2, this.commandService.getClustersForCommand(COMMAND_3_ID, null).size());

        this.service.deleteCluster(CLUSTER_1_ID);

        Assert.assertEquals(1, this.commandService.getClustersForCommand(COMMAND_1_ID, null).size());
        Assert.assertEquals(1,
            this.commandService.getClustersForCommand(COMMAND_1_ID, null)
                .stream()
                .filter(cluster -> CLUSTER_2_ID.equals(cluster.getId().orElseThrow(IllegalArgumentException::new)))
                .count()
        );
        Assert.assertEquals(1, this.commandService.getClustersForCommand(COMMAND_2_ID, null).size());
        Assert.assertEquals(1,
            this.commandService.getClustersForCommand(COMMAND_2_ID, null)
                .stream()
                .filter(cluster -> CLUSTER_2_ID.equals(cluster.getId().orElseThrow(IllegalArgumentException::new)))
                .count()
        );
        Assert.assertEquals(1, this.commandService.getClustersForCommand(COMMAND_3_ID, null).size());
        Assert.assertEquals(1,
            this.commandService.getClustersForCommand(COMMAND_3_ID, null)
                .stream()
                .filter(cluster -> CLUSTER_2_ID.equals(cluster.getId().orElseThrow(IllegalArgumentException::new)))
                .count()
        );
    }

    /**
     * Test add configurations to cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddConfigsToCluster() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assert.assertEquals(1, this.service.getConfigsForCluster(CLUSTER_1_ID).size());
        this.service.addConfigsForCluster(CLUSTER_1_ID, newConfigs);
        final Set<String> finalConfigs = this.service.getConfigsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(4, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test update dependencies for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateConfigsForCluster() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assert.assertEquals(1, this.service.getConfigsForCluster(CLUSTER_1_ID).size());
        this.service.updateConfigsForCluster(CLUSTER_1_ID, newConfigs);
        final Set<String> finalConfigs = this.service.getConfigsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(3, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test get configurations for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetConfigsForCluster() throws GenieException {
        Assert.assertEquals(1,
            this.service.getConfigsForCluster(CLUSTER_1_ID).size());
    }

    /**
     * Test add dependencies to cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddDependenciesToCluster() throws GenieException {
        final String newDep1 = UUID.randomUUID().toString();
        final String newDep2 = UUID.randomUUID().toString();
        final String newDep3 = UUID.randomUUID().toString();

        final Set<String> newDeps = Sets.newHashSet(newDep1, newDep2, newDep3);

        Assert.assertEquals(2, this.service.getDependenciesForCluster(CLUSTER_1_ID).size());
        this.service.addDependenciesForCluster(CLUSTER_1_ID, newDeps);
        final Set<String> finalDeps = this.service.getDependenciesForCluster(CLUSTER_1_ID);
        Assert.assertEquals(5, finalDeps.size());
        Assert.assertTrue(finalDeps.contains(newDep1));
        Assert.assertTrue(finalDeps.contains(newDep2));
        Assert.assertTrue(finalDeps.contains(newDep3));
    }

    /**
     * Test update configurations for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateDependenciesForCluster() throws GenieException {
        final String newDep1 = UUID.randomUUID().toString();
        final String newDep2 = UUID.randomUUID().toString();
        final String newDep3 = UUID.randomUUID().toString();

        final Set<String> newDependencies = Sets.newHashSet(newDep1, newDep2, newDep3);

        Assert.assertEquals(2, this.service.getDependenciesForCluster(CLUSTER_1_ID).size());
        this.service.updateDependenciesForCluster(CLUSTER_1_ID, newDependencies);
        final Set<String> finalConfigs = this.service.getDependenciesForCluster(CLUSTER_1_ID);
        Assert.assertEquals(3, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newDep1));
        Assert.assertTrue(finalConfigs.contains(newDep2));
        Assert.assertTrue(finalConfigs.contains(newDep3));
    }

    /**
     * Test get configurations for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetDependenciesForCluster() throws GenieException {
        Assert.assertEquals(2,
            this.service.getDependenciesForCluster(CLUSTER_1_ID).size());
    }

    /**
     * Test adding commands to the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddCommandsForCluster() throws GenieException {
        final String command1Id = this.commandService.createCommand(
            new Command.Builder(
                "name",
                "user",
                "23.1.0",
                CommandStatus.ACTIVE,
                "pig",
                8108123L
            ).build()
        );
        final String command2Id = this.commandService.createCommand(
            new Command.Builder(
                "name2",
                "user2",
                "23.1.1",
                CommandStatus.INACTIVE,
                "pig2",
                8023423L
            ).build()
        );
        final List<String> newCommandIds = new ArrayList<>();
        newCommandIds.add(command1Id);
        newCommandIds.add(command2Id);
        Assert.assertEquals(3, this.service.getCommandsForCluster(CLUSTER_1_ID, null).size());
        this.service.addCommandsForCluster(CLUSTER_1_ID, newCommandIds);
        final List<Command> commands = this.service.getCommandsForCluster(CLUSTER_1_ID, null);
        Assert.assertEquals(5, commands.size());
        Assert.assertEquals(command1Id, commands.get(3).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(command2Id, commands.get(4).getId().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test the Get commands for cluster function.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetCommandsForCluster() throws GenieException {
        final List<Command> commands
            = this.service.getCommandsForCluster(CLUSTER_1_ID, null);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_1_ID, commands.get(0).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(COMMAND_2_ID, commands.get(2).getId().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test the Get clusters for cluster function.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetCommandsForClusterNoId() throws GenieException {
        this.service.getCommandsForCluster("", null);
    }

    /**
     * Test updating commands for the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateCommandsForCluster() throws GenieException {
        final String command1Id = this.commandService.createCommand(
            new Command.Builder(
                "name",
                "user",
                "23.1.0",
                CommandStatus.ACTIVE,
                "pig",
                137324L
            ).build()
        );
        final String command2Id = this.commandService.createCommand(
            new Command.Builder(
                "name2",
                "user2",
                "23.1.1",
                CommandStatus.INACTIVE,
                "pig2",
                23423L
            ).build()
        );
        final List<String> newCommandIds = new ArrayList<>();
        newCommandIds.add(command1Id);
        newCommandIds.add(command2Id);
        Assert.assertEquals(3, this.service.getCommandsForCluster(CLUSTER_1_ID, null).size());
        this.service.setCommandsForCluster(CLUSTER_1_ID, newCommandIds);
        final List<Command> commands = this.service.getCommandsForCluster(CLUSTER_1_ID, null);
        Assert.assertEquals(2, commands.size());
        Assert.assertEquals(command1Id, commands.get(0).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(command2Id, commands.get(1).getId().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllCommandsForCluster() throws GenieException {
        Assert.assertEquals(3, this.service.getCommandsForCluster(CLUSTER_1_ID, null).size());
        this.service.removeAllCommandsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(0, this.service.getCommandsForCluster(CLUSTER_1_ID, null).size());
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveCommandForCluster() throws GenieException {
        Assert.assertEquals(3, this.service.getCommandsForCluster(CLUSTER_1_ID, null).size());
        this.service.removeCommandForCluster(CLUSTER_1_ID, COMMAND_1_ID);
        Assert.assertEquals(2, this.service.getCommandsForCluster(CLUSTER_1_ID, null).size());
    }

    /**
     * Test add tags to cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddTagsToCluster() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assert.assertEquals(5, this.service.getTagsForCluster(CLUSTER_1_ID).size());
        this.service.addTagsForCluster(CLUSTER_1_ID, newTags);
        final Set<String> finalTags = this.service.getTagsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(8, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test update tags for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateTagsForCluster() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assert.assertEquals(5, this.service.getTagsForCluster(CLUSTER_1_ID).size());
        this.service.updateTagsForCluster(CLUSTER_1_ID, newTags);
        final Set<String> finalTags = this.service.getTagsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(5, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test get tags for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetTagsForCluster() throws GenieException {
        Assert.assertEquals(5,
            this.service.getTagsForCluster(CLUSTER_1_ID).size());
    }

    /**
     * Test remove all tags for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllTagsForCluster() throws GenieException {
        Assert.assertEquals(5, this.service.getTagsForCluster(CLUSTER_1_ID).size());
        this.service.removeAllTagsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(2, this.service.getTagsForCluster(CLUSTER_1_ID).size());
    }

    /**
     * Test remove tag for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveTagForCluster() throws GenieException {
        Assert.assertTrue(this.service.getTagsForCluster(CLUSTER_1_ID).contains("prod"));
        this.service.removeTagForCluster(CLUSTER_1_ID, "prod");
        Assert.assertFalse(this.service.getTagsForCluster(CLUSTER_1_ID).contains("prod"));
    }

    /**
     * Test the API for deleting all terminated clusters that aren't attached to jobs.
     *
     * @throws GenieException On Error
     * @throws IOException    On JSON patch error
     */
    @Test
    public void testDeleteTerminatedClusters() throws GenieException, IOException {
        Assert.assertThat(this.clusterRepository.count(), Matchers.is(2L));
        final String testClusterId = UUID.randomUUID().toString();
        final Cluster testCluster = new Cluster.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            ClusterStatus.OUT_OF_SERVICE
        )
            .withId(testClusterId)
            .withConfigs(Sets.newHashSet(UUID.randomUUID().toString()))
            .withDependencies(Sets.newHashSet(UUID.randomUUID().toString()))
            .withSetupFile(UUID.randomUUID().toString())
            .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            .build();
        this.service.createCluster(testCluster);

        // Shouldn't delete any clusters as all are UP or OOS
        Assert.assertThat(this.service.deleteTerminatedClusters(), Matchers.is(0));

        // Change status to UP
        String patchString = "[{ \"op\": \"replace\", \"path\": \"/status\", \"value\": \"UP\" }]";
        JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));
        this.service.patchCluster(testClusterId, patch);
        Assert.assertThat(this.service.getCluster(testClusterId).getStatus(), Matchers.is(ClusterStatus.UP));

        // All clusters are UP/OOS or attached to jobs
        Assert.assertThat(this.service.deleteTerminatedClusters(), Matchers.is(0));

        // Change status to terminated
        patchString = "[{ \"op\": \"replace\", \"path\": \"/status\", \"value\": \"TERMINATED\" }]";
        patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));
        this.service.patchCluster(testClusterId, patch);
        Assert.assertThat(this.service.getCluster(testClusterId).getStatus(), Matchers.is(ClusterStatus.TERMINATED));

        // All clusters are UP/OOS or attached to jobs
        Assert.assertThat(this.service.deleteTerminatedClusters(), Matchers.is(1));

        // Make sure it didn't delete any of the clusters we wanted
        Assert.assertTrue(this.clusterRepository.existsByUniqueId(CLUSTER_1_ID));
        Assert.assertTrue(this.clusterRepository.existsByUniqueId(CLUSTER_2_ID));
        Assert.assertFalse(this.clusterRepository.existsByUniqueId(testClusterId));
    }
}
