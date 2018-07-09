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
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.ClusterMetadata;
import com.netflix.genie.common.internal.dto.v4.ClusterRequest;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.CommandMetadata;
import com.netflix.genie.common.internal.dto.v4.CommandRequest;
import com.netflix.genie.common.internal.dto.v4.Criterion;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
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
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the CommandServiceJPAImpl.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(IntegrationTest.class)
@DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTests/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaClusterPersistenceServiceImplIntegrationTests extends DBIntegrationTestBase {

    private static final String COMMAND_1_ID = "command1";
    private static final String COMMAND_2_ID = "command2";
    private static final String COMMAND_3_ID = "command3";
    private static final String COMMAND_4_ID = "command4";

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

    private static final Pageable PAGE = PageRequest.of(0, 10, Sort.Direction.DESC, "updated");

    @Autowired
    private ClusterPersistenceService service;

    @Autowired
    private CommandPersistenceService commandPersistenceService;

    /**
     * Test the get cluster method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetCluster() throws GenieException {
        final Cluster cluster1 = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_1_ID, cluster1.getId());
        Assert.assertEquals(CLUSTER_1_NAME, cluster1.getMetadata().getName());
        Assert.assertEquals(CLUSTER_1_USER, cluster1.getMetadata().getUser());
        Assert.assertEquals(CLUSTER_1_VERSION, cluster1.getMetadata().getVersion());
        Assert.assertEquals(CLUSTER_1_STATUS, cluster1.getMetadata().getStatus());
        Assert.assertEquals(3, cluster1.getMetadata().getTags().size());
        Assert.assertEquals(1, cluster1.getResources().getConfigs().size());
        Assert.assertEquals(2, cluster1.getResources().getDependencies().size());

        final Cluster cluster2 = this.service.getCluster(CLUSTER_2_ID);
        Assert.assertEquals(CLUSTER_2_ID, cluster2.getId());
        Assert.assertEquals(CLUSTER_2_NAME, cluster2.getMetadata().getName());
        Assert.assertEquals(CLUSTER_2_USER, cluster2.getMetadata().getUser());
        Assert.assertEquals(CLUSTER_2_VERSION, cluster2.getMetadata().getVersion());
        Assert.assertEquals(CLUSTER_2_STATUS, cluster2.getMetadata().getStatus());
        Assert.assertEquals(3, cluster2.getMetadata().getTags().size());
        Assert.assertEquals(2, cluster2.getResources().getConfigs().size());
        Assert.assertEquals(0, cluster2.getResources().getDependencies().size());
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByName() {
        final Page<Cluster> clusters = this.service.getClusters(CLUSTER_2_NAME, null, null, null, null, PAGE);
        Assert.assertEquals(1, clusters.getNumberOfElements());
        Assert.assertEquals(CLUSTER_2_ID, clusters.getContent().get(0).getId());
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
        Assert.assertEquals(CLUSTER_2_ID, clusters.getContent().get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.getContent().get(1).getId());
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByTags() {
        final Set<String> tags = Sets.newHashSet("prod");
        Page<Cluster> clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assert.assertEquals(1, clusters.getNumberOfElements());
        Assert.assertEquals(CLUSTER_1_ID, clusters.getContent().get(0).getId());

        tags.clear();
        tags.add("hive");
        clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(CLUSTER_2_ID, clusters.getContent().get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.getContent().get(1).getId());

        tags.add("somethingThatWouldNeverReallyExist");
        clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assert.assertTrue(clusters.getContent().isEmpty());

        tags.clear();
        clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(CLUSTER_2_ID, clusters.getContent().get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.getContent().get(1).getId());
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
        Assert.assertEquals(CLUSTER_2_ID, clusters.getContent().get(0).getId());
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
        Assert.assertEquals(CLUSTER_1_ID, clusters.getContent().get(0).getId());
    }

    /**
     * Test the get clusters method with descending sort.
     */
    @Test
    public void testGetClustersDescending() {
        //Default to order by Updated
        final Page<Cluster> clusters = this.service.getClusters(null, null, null, null, null, PAGE);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(CLUSTER_2_ID, clusters.getContent().get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.getContent().get(1).getId());
    }

    /**
     * Test the get clusters method with ascending sort.
     */
    @Test
    public void testGetClustersAscending() {
        final Pageable ascendingPage = PageRequest.of(0, 10, Sort.Direction.ASC, "updated");
        //Default to order by Updated
        final Page<Cluster> clusters = this.service.getClusters(null, null, null, null, null, ascendingPage);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(CLUSTER_1_ID, clusters.getContent().get(0).getId());
        Assert.assertEquals(CLUSTER_2_ID, clusters.getContent().get(1).getId());
    }

    /**
     * Test the get clusters method order by name.
     */
    @Test
    public void testGetClustersOrderBysUser() {
        final Pageable userPage = PageRequest.of(0, 10, Sort.Direction.DESC, "user");
        final Page<Cluster> clusters = this.service.getClusters(null, null, null, null, null, userPage);
        Assert.assertEquals(2, clusters.getNumberOfElements());
        Assert.assertEquals(CLUSTER_1_ID, clusters.getContent().get(0).getId());
        Assert.assertEquals(CLUSTER_2_ID, clusters.getContent().get(1).getId());
    }

    /**
     * Test the get clusters method order by an invalid field should return the order by default value (updated).
     */
    @Test(expected = RuntimeException.class)
    public void testGetClustersOrderBysInvalidField() {
        final Pageable badPage = PageRequest.of(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
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
     * Test the choseClusterForJobRequest function.
     *
     * @throws GenieException For any problem
     */
    @Test
    @SuppressWarnings("checkstyle:methodlength")
    // TODO: This would be much easier with a spock data test
    public void testChooseClusterAndCommandForCriteria() throws GenieException {
        Map<Cluster, String> clustersAndCommands;

        // All Good
        clustersAndCommands = this.service.findClustersAndCommandsForCriteria(
            Lists.newArrayList(
                new Criterion
                    .Builder()
                    .withId(CLUSTER_1_ID)
                    .build()
            ),
            new Criterion
                .Builder()
                .withTags(Sets.newHashSet("pig"))
                .build()
        );
        Assert.assertThat(clustersAndCommands.size(), Matchers.is(1));
        Assert.assertThat(
            clustersAndCommands
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().getId().equals(CLUSTER_1_ID))
                .count(),
            Matchers.is(1L));
        Assert.assertTrue(clustersAndCommands.containsValue(COMMAND_1_ID));

        // Cluster id won't be found
        clustersAndCommands = this.service.findClustersAndCommandsForCriteria(
            Lists.newArrayList(
                new Criterion
                    .Builder()
                    .withId(CLUSTER_1_ID + UUID.randomUUID().toString())
                    .build()
            ),
            new Criterion
                .Builder()
                .withTags(Sets.newHashSet("pig"))
                .build()
        );
        Assert.assertTrue(clustersAndCommands.isEmpty());

        // Cluster is UP so this should fail even though the id matches
        clustersAndCommands = this.service.findClustersAndCommandsForCriteria(
            Lists.newArrayList(
                new Criterion
                    .Builder()
                    .withId(CLUSTER_1_ID)
                    .withStatus(ClusterStatus.OUT_OF_SERVICE.toString())
                    .build()
            ),
            new Criterion
                .Builder()
                .withTags(Sets.newHashSet("pig"))
                .build()
        );
        Assert.assertTrue(clustersAndCommands.isEmpty());

        // Second cluster criterion should match Cluster 2
        clustersAndCommands = this.service.findClustersAndCommandsForCriteria(
            Lists.newArrayList(
                new Criterion
                    .Builder()
                    .withId(CLUSTER_1_ID)
                    .withStatus(ClusterStatus.OUT_OF_SERVICE.toString())
                    .build(),
                new Criterion
                    .Builder()
                    .withTags(Sets.newHashSet("hive", "pig", "query"))
                    .build()
            ),
            new Criterion
                .Builder()
                .withId(COMMAND_1_ID)
                .withTags(Sets.newHashSet("pig"))
                .build()
        );
        Assert.assertThat(clustersAndCommands.size(), Matchers.is(1));
        Assert.assertThat(
            clustersAndCommands
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().getId().equals(CLUSTER_2_ID))
                .count(),
            Matchers.is(1L));
        Assert.assertTrue(clustersAndCommands.containsValue(COMMAND_1_ID));

        // Matches both clusters and the deprecated pig command
        clustersAndCommands = this.service.findClustersAndCommandsForCriteria(
            Lists.newArrayList(
                new Criterion
                    .Builder()
                    .withTags(Sets.newHashSet("hive", "pig"))
                    .build()
            ),
            new Criterion
                .Builder()
                .withStatus(CommandStatus.DEPRECATED.toString())
                .withTags(Sets.newHashSet("pig"))
                .build()
        );
        Assert.assertThat(clustersAndCommands.size(), Matchers.is(2));
        Assert.assertThat(
            clustersAndCommands
                .entrySet()
                .stream()
                .filter(
                    entry -> entry.getKey().getId().equals(CLUSTER_2_ID) || entry.getKey().getId().equals(CLUSTER_1_ID)
                )
                .count(),
            Matchers.is(2L));
        Assert.assertTrue(clustersAndCommands.containsValue(COMMAND_3_ID));
        Assert.assertFalse(clustersAndCommands.containsValue(COMMAND_1_ID));
        Assert.assertFalse(clustersAndCommands.containsValue(COMMAND_2_ID));

        // By name
        clustersAndCommands = this.service.findClustersAndCommandsForCriteria(
            Lists.newArrayList(
                new Criterion
                    .Builder()
                    .withName(CLUSTER_2_NAME)
                    .build()
            ),
            new Criterion
                .Builder()
                .withName("hive_11_prod")
                .withStatus(CommandStatus.INACTIVE.toString())
                .build()
        );
        Assert.assertThat(clustersAndCommands.size(), Matchers.is(1));
        Assert.assertThat(
            clustersAndCommands
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().getId().equals(CLUSTER_2_ID))
                .count(),
            Matchers.is(1L));
        Assert.assertTrue(clustersAndCommands.containsValue(COMMAND_2_ID));

        // In this case we're testing the priority ordering. The two clusters have the same pig command only cluster 2
        // has a different pig command in the highest priority order so the result set should be different for both
        clustersAndCommands = this.service.findClustersAndCommandsForCriteria(
            Lists.newArrayList(
                new Criterion
                    .Builder()
                    .withTags(Sets.newHashSet("hive", "pig"))
                    .build()
            ),
            new Criterion
                .Builder()
                .withTags(Sets.newHashSet("pig"))
                .build()
        );
        Assert.assertThat(clustersAndCommands.size(), Matchers.is(2));
        clustersAndCommands.forEach(
            (key, value) -> {
                switch (key.getId()) {
                    case CLUSTER_1_ID:
                        Assert.assertThat(value, Matchers.is(COMMAND_1_ID));
                        break;
                    case CLUSTER_2_ID:
                        Assert.assertThat(value, Matchers.is(COMMAND_4_ID));
                        break;
                    default:
                        Assert.fail();
                        break;
                }
            }
        );

        // Search by version will break standard find
        clustersAndCommands = this.service.findClustersAndCommandsForCriteria(
            Lists.newArrayList(
                new Criterion
                    .Builder()
                    .withId(CLUSTER_1_ID)
                    .withVersion(UUID.randomUUID().toString())
                    .build()
            ),
            new Criterion
                .Builder()
                .withTags(Sets.newHashSet("pig"))
                .build()
        );
        Assert.assertThat(clustersAndCommands.size(), Matchers.is(0));

        // Versions match so it'll match like the very first case
        clustersAndCommands = this.service.findClustersAndCommandsForCriteria(
            Lists.newArrayList(
                new Criterion
                    .Builder()
                    .withId(CLUSTER_1_ID)
                    .withVersion(CLUSTER_1_VERSION)
                    .build()
            ),
            new Criterion
                .Builder()
                .withVersion("7.8.9")
                .withTags(Sets.newHashSet("pig"))
                .withStatus(CommandStatus.DEPRECATED.toString())
                .build()
        );
        Assert.assertThat(clustersAndCommands.size(), Matchers.is(1));
        Assert.assertThat(
            clustersAndCommands
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().getId().equals(CLUSTER_1_ID))
                .count(),
            Matchers.is(1L));
        Assert.assertTrue(clustersAndCommands.containsValue(COMMAND_3_ID));
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
        final ClusterRequest cluster = new ClusterRequest.Builder(
            new ClusterMetadata.Builder(
                CLUSTER_1_NAME,
                CLUSTER_1_USER,
                CLUSTER_1_VERSION,
                ClusterStatus.OUT_OF_SERVICE
            )
                .build()
        )
            .withRequestedId(id)
            .withResources(new ExecutionEnvironment(configs, dependencies, null))
            .build();
        this.service.createCluster(cluster);
        final Cluster created = this.service.getCluster(id);
        Assert.assertNotNull(created);
        Assert.assertEquals(id, created.getId());
        Assert.assertEquals(CLUSTER_1_NAME, created.getMetadata().getName());
        Assert.assertEquals(CLUSTER_1_USER, created.getMetadata().getUser());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, created.getMetadata().getStatus());
        Assert.assertEquals(3, created.getResources().getConfigs().size());
        Assert.assertEquals(1, created.getResources().getDependencies().size());
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
        final ClusterRequest cluster = new ClusterRequest.Builder(
            new ClusterMetadata.Builder(
                CLUSTER_1_NAME,
                CLUSTER_1_USER,
                CLUSTER_1_VERSION,
                ClusterStatus.OUT_OF_SERVICE
            )
                .build()
        )
            .withResources(new ExecutionEnvironment(configs, dependencies, null))
            .build();
        final String id = this.service.createCluster(cluster);
        final Cluster created = this.service.getCluster(id);
        Assert.assertEquals(CLUSTER_1_NAME, created.getMetadata().getName());
        Assert.assertEquals(CLUSTER_1_USER, created.getMetadata().getUser());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, created.getMetadata().getStatus());
        Assert.assertEquals(3, created.getResources().getConfigs().size());
        Assert.assertEquals(1, created.getResources().getDependencies().size());
        this.service.deleteCluster(created.getId());
        try {
            this.service.getCluster(created.getId());
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
        Assert.assertEquals(CLUSTER_1_USER, getCluster.getMetadata().getUser());
        Assert.assertEquals(ClusterStatus.UP, getCluster.getMetadata().getStatus());
        Assert.assertEquals(3, getCluster.getMetadata().getTags().size());

        final Set<String> tags = Sets.newHashSet("tez", "yarn", "hadoop");
        tags.addAll(getCluster.getMetadata().getTags());

        final Cluster updateCluster = new Cluster(
            getCluster.getId(),
            getCluster.getCreated(),
            getCluster.getUpdated(),
            new ExecutionEnvironment(
                getCluster.getResources().getConfigs(),
                getCluster.getResources().getDependencies(),
                getCluster.getResources().getSetupFile().orElse(null)
            ),
            new ClusterMetadata.Builder(
                getCluster.getMetadata().getName(),
                CLUSTER_2_USER,
                getCluster.getMetadata().getVersion(),
                ClusterStatus.OUT_OF_SERVICE
            )
                .withTags(tags)
                .withDescription(getCluster.getMetadata().getDescription().orElse(null))
                .build()
        );

        this.service.updateCluster(CLUSTER_1_ID, updateCluster);

        final Cluster updated = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_2_USER, updated.getMetadata().getUser());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, updated.getMetadata().getStatus());
        Assert.assertEquals(6, updated.getMetadata().getTags().size());
    }

    /**
     * Test to update a cluster with invalid content. Should throw ConstraintViolationException from JPA layer.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateClusterWithInvalidCluster() throws GenieException {
        final Cluster getCluster = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_1_USER, getCluster.getMetadata().getUser());
        Assert.assertEquals(ClusterStatus.UP, getCluster.getMetadata().getStatus());
        Assert.assertEquals(3, getCluster.getMetadata().getTags().size());

        final Cluster updateCluster = new Cluster(
            getCluster.getId(),
            getCluster.getCreated(),
            getCluster.getUpdated(),
            new ExecutionEnvironment(
                getCluster.getResources().getConfigs(),
                getCluster.getResources().getDependencies(),
                getCluster.getResources().getSetupFile().orElse(null)
            ),
            new ClusterMetadata.Builder(
                "",
                getCluster.getMetadata().getUser(),
                getCluster.getMetadata().getVersion(),
                ClusterStatus.OUT_OF_SERVICE
            )
                .withTags(getCluster.getMetadata().getTags())
                .withDescription(getCluster.getMetadata().getDescription().orElse(null))
                .build()
        );

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
        final Instant created = getCluster.getCreated();
        final Instant updated = getCluster.getUpdated();

        final Cluster updateCluster = new Cluster(
            getCluster.getId(),
            Instant.now(),
            Instant.EPOCH,
            new ExecutionEnvironment(
                getCluster.getResources().getConfigs(),
                getCluster.getResources().getDependencies(),
                getCluster.getResources().getSetupFile().orElse(null)
            ),
            new ClusterMetadata.Builder(
                getCluster.getMetadata().getUser(),
                getCluster.getMetadata().getUser(),
                getCluster.getMetadata().getVersion(),
                getCluster.getMetadata().getStatus()
            )
                .withTags(getCluster.getMetadata().getTags())
                .withDescription(getCluster.getMetadata().getDescription().orElse(null))
                .build()
        );

        this.service.updateCluster(CLUSTER_1_ID, updateCluster);
        final Cluster updatedCluster = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(created, updatedCluster.getCreated());
        Assert.assertNotEquals(updated, updatedCluster.getUpdated());
        Assert.assertNotEquals(Instant.EPOCH, updatedCluster.getUpdated());
        Assert.assertEquals(getCluster.getMetadata().getTags(), updatedCluster.getMetadata().getTags());
        Assert.assertEquals(getCluster.getResources().getConfigs(), updatedCluster.getResources().getConfigs());
        Assert.assertEquals(
            getCluster.getResources().getDependencies(),
            updatedCluster.getResources().getDependencies()
        );
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
        Assert.assertThat(getCluster.getMetadata().getName(), Matchers.is(CLUSTER_1_NAME));
        final Instant updateTime = getCluster.getUpdated();

        final String patchString
            = "[{ \"op\": \"replace\", \"path\": \"/metadata/name\", \"value\": \"" + CLUSTER_2_NAME + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));

        this.service.patchCluster(CLUSTER_1_ID, patch);

        final Cluster updated = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertNotEquals(updated.getUpdated(), Matchers.is(updateTime));
        Assert.assertThat(updated.getMetadata().getName(), Matchers.is(CLUSTER_2_NAME));
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
        Assert.assertEquals(2, this.commandPersistenceService.getClustersForCommand(COMMAND_1_ID, null).size());
        Assert.assertEquals(2, this.commandPersistenceService.getClustersForCommand(COMMAND_2_ID, null).size());
        Assert.assertEquals(2, this.commandPersistenceService.getClustersForCommand(COMMAND_3_ID, null).size());

        this.service.deleteCluster(CLUSTER_1_ID);

        Assert.assertEquals(1, this.commandPersistenceService.getClustersForCommand(COMMAND_1_ID, null).size());
        Assert.assertEquals(1,
            this.commandPersistenceService.getClustersForCommand(COMMAND_1_ID, null)
                .stream()
                .filter(cluster -> CLUSTER_2_ID.equals(cluster.getId()))
                .count()
        );
        Assert.assertEquals(1, this.commandPersistenceService.getClustersForCommand(COMMAND_2_ID, null).size());
        Assert.assertEquals(1,
            this.commandPersistenceService.getClustersForCommand(COMMAND_2_ID, null)
                .stream()
                .filter(cluster -> CLUSTER_2_ID.equals(cluster.getId()))
                .count()
        );
        Assert.assertEquals(1, this.commandPersistenceService.getClustersForCommand(COMMAND_3_ID, null).size());
        Assert.assertEquals(1,
            this.commandPersistenceService.getClustersForCommand(COMMAND_3_ID, null)
                .stream()
                .filter(cluster -> CLUSTER_2_ID.equals(cluster.getId()))
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
        final String command1Id = this.commandPersistenceService.createCommand(
            new CommandRequest.Builder(
                new CommandMetadata.Builder(
                    "name",
                    "user",
                    "23.1.0",
                    CommandStatus.ACTIVE
                )
                    .build(),
                Lists.newArrayList("pig")
            )
                .withCheckDelay(8108123L)
                .build()
        );
        final String command2Id = this.commandPersistenceService.createCommand(
            new CommandRequest.Builder(
                new CommandMetadata.Builder(
                    "name2",
                    "user2",
                    "23.1.1",
                    CommandStatus.INACTIVE
                )
                    .build(),
                Lists.newArrayList("pig2")
            )
                .withCheckDelay(8023423L)
                .build()
        );
        final List<String> newCommandIds = new ArrayList<>();
        newCommandIds.add(command1Id);
        newCommandIds.add(command2Id);
        Assert.assertEquals(3, this.service.getCommandsForCluster(CLUSTER_1_ID, null).size());
        this.service.addCommandsForCluster(CLUSTER_1_ID, newCommandIds);
        final List<Command> commands = this.service.getCommandsForCluster(CLUSTER_1_ID, null);
        Assert.assertEquals(5, commands.size());
        Assert.assertEquals(command1Id, commands.get(3).getId());
        Assert.assertEquals(command2Id, commands.get(4).getId());
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
        Assert.assertEquals(COMMAND_1_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_2_ID, commands.get(2).getId());
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
        final String command1Id = this.commandPersistenceService.createCommand(
            new CommandRequest.Builder(
                new CommandMetadata.Builder(
                    "name",
                    "user",
                    "23.1.0",
                    CommandStatus.ACTIVE
                )
                    .build(),
                Lists.newArrayList("pig")
            )
                .withCheckDelay(137324L)
                .build()
        );
        final String command2Id = this.commandPersistenceService.createCommand(
            new CommandRequest.Builder(
                new CommandMetadata.Builder(
                    "name2",
                    "user2",
                    "23.1.1",
                    CommandStatus.INACTIVE
                )
                    .build(),
                Lists.newArrayList("pig2")
            )
                .withCheckDelay(23423L)
                .build()
        );
        final List<String> newCommandIds = new ArrayList<>();
        newCommandIds.add(command1Id);
        newCommandIds.add(command2Id);
        Assert.assertEquals(3, this.service.getCommandsForCluster(CLUSTER_1_ID, null).size());
        this.service.setCommandsForCluster(CLUSTER_1_ID, newCommandIds);
        final List<Command> commands = this.service.getCommandsForCluster(CLUSTER_1_ID, null);
        Assert.assertEquals(2, commands.size());
        Assert.assertEquals(command1Id, commands.get(0).getId());
        Assert.assertEquals(command2Id, commands.get(1).getId());
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

        Assert.assertEquals(3, this.service.getTagsForCluster(CLUSTER_1_ID).size());
        this.service.addTagsForCluster(CLUSTER_1_ID, newTags);
        final Set<String> finalTags = this.service.getTagsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(6, finalTags.size());
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

        Assert.assertEquals(3, this.service.getTagsForCluster(CLUSTER_1_ID).size());
        this.service.updateTagsForCluster(CLUSTER_1_ID, newTags);
        final Set<String> finalTags = this.service.getTagsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(3, finalTags.size());
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
        Assert.assertEquals(3, this.service.getTagsForCluster(CLUSTER_1_ID).size());
    }

    /**
     * Test remove all tags for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllTagsForCluster() throws GenieException {
        Assert.assertEquals(3, this.service.getTagsForCluster(CLUSTER_1_ID).size());
        this.service.removeAllTagsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(0, this.service.getTagsForCluster(CLUSTER_1_ID).size());
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
        final ClusterRequest testCluster = new ClusterRequest.Builder(
            new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.OUT_OF_SERVICE
            )
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build()
        )
            .withRequestedId(testClusterId)
            .withResources(
                new ExecutionEnvironment(
                    Sets.newHashSet(UUID.randomUUID().toString()),
                    Sets.newHashSet(UUID.randomUUID().toString()),
                    UUID.randomUUID().toString())
            ).build();
        this.service.createCluster(testCluster);

        // Shouldn't delete any clusters as all are UP or OOS
        Assert.assertThat(this.service.deleteTerminatedClusters(), Matchers.is(0L));

        // Change status to UP
        String patchString = "[{ \"op\": \"replace\", \"path\": \"/metadata/status\", \"value\": \"UP\" }]";
        JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));
        this.service.patchCluster(testClusterId, patch);
        Assert.assertThat(
            this.service.getCluster(testClusterId).getMetadata().getStatus(),
            Matchers.is(ClusterStatus.UP)
        );

        // All clusters are UP/OOS or attached to jobs
        Assert.assertThat(this.service.deleteTerminatedClusters(), Matchers.is(0L));

        // Change status to terminated
        patchString = "[{ \"op\": \"replace\", \"path\": \"/metadata/status\", \"value\": \"TERMINATED\" }]";
        patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));
        this.service.patchCluster(testClusterId, patch);
        Assert.assertThat(
            this.service.getCluster(testClusterId).getMetadata().getStatus(),
            Matchers.is(ClusterStatus.TERMINATED)
        );

        // All clusters are UP/OOS or attached to jobs
        Assert.assertThat(this.service.deleteTerminatedClusters(), Matchers.is(1L));

        // Make sure it didn't delete any of the clusters we wanted
        Assert.assertTrue(this.clusterRepository.existsByUniqueId(CLUSTER_1_ID));
        Assert.assertTrue(this.clusterRepository.existsByUniqueId(CLUSTER_2_ID));
        Assert.assertFalse(this.clusterRepository.existsByUniqueId(testClusterId));
    }
}
