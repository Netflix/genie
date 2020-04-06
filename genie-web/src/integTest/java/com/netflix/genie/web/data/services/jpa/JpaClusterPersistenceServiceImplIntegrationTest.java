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
package com.netflix.genie.web.data.services.jpa;

import com.github.fge.jsonpatch.JsonPatch;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata;
import com.netflix.genie.common.external.dtos.v4.ClusterRequest;
import com.netflix.genie.common.external.dtos.v4.ClusterStatus;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.CommandMetadata;
import com.netflix.genie.common.external.dtos.v4.CommandRequest;
import com.netflix.genie.common.external.dtos.v4.CommandStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.web.data.services.CommandPersistenceService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
import java.io.IOException;
import java.time.Instant;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for {@link JpaClusterPersistenceServiceImpl}.
 *
 * @author tgianos
 * @since 2.0.0
 */
@DatabaseTearDown("cleanup.xml")
class JpaClusterPersistenceServiceImplIntegrationTest extends DBIntegrationTestBase {

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
    private JpaClusterPersistenceServiceImpl service;

    @Autowired
    private CommandPersistenceService commandPersistenceService;

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetCluster() throws GenieException {
        final Cluster cluster1 = this.service.getCluster(CLUSTER_1_ID);
        Assertions.assertThat(cluster1.getId()).isEqualTo(CLUSTER_1_ID);
        final ClusterMetadata cluster1Metadata = cluster1.getMetadata();
        Assertions.assertThat(cluster1Metadata.getName()).isEqualTo(CLUSTER_1_NAME);
        Assertions.assertThat(cluster1Metadata.getUser()).isEqualTo(CLUSTER_1_USER);
        Assertions.assertThat(cluster1Metadata.getVersion()).isEqualTo(CLUSTER_1_VERSION);
        Assertions.assertThat(cluster1Metadata.getStatus()).isEqualByComparingTo(CLUSTER_1_STATUS);
        Assertions.assertThat(cluster1Metadata.getTags()).hasSize(3);
        Assertions.assertThat(cluster1.getResources().getConfigs()).hasSize(1);
        Assertions.assertThat(cluster1.getResources().getDependencies()).hasSize(2);

        final Cluster cluster2 = this.service.getCluster(CLUSTER_2_ID);
        Assertions.assertThat(cluster2.getId()).isEqualTo(CLUSTER_2_ID);
        final ClusterMetadata cluster2Metadata = cluster2.getMetadata();
        Assertions.assertThat(cluster2Metadata.getName()).isEqualTo(CLUSTER_2_NAME);
        Assertions.assertThat(cluster2Metadata.getUser()).isEqualTo(CLUSTER_2_USER);
        Assertions.assertThat(cluster2Metadata.getVersion()).isEqualTo(CLUSTER_2_VERSION);
        Assertions.assertThat(cluster2Metadata.getStatus()).isEqualByComparingTo(CLUSTER_2_STATUS);
        Assertions.assertThat(cluster2Metadata.getTags()).hasSize(3);
        Assertions.assertThat(cluster2.getResources().getConfigs()).hasSize(2);
        Assertions.assertThat(cluster2.getResources().getDependencies()).isEmpty();
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetClustersByName() {
        final Page<Cluster> clusters = this.service.getClusters(CLUSTER_2_NAME, null, null, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetClustersByStatuses() {
        final Set<ClusterStatus> statuses = EnumSet.of(ClusterStatus.UP);
        final Page<Cluster> clusters = this.service.getClusters(null, statuses, null, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_1_ID);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetClustersByTags() {
        final Set<String> tags = Sets.newHashSet("prod");
        Page<Cluster> clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_1_ID);

        tags.clear();
        tags.add("hive");
        clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_1_ID);

        tags.add("somethingThatWouldNeverReallyExist");
        clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assertions.assertThat(clusters.getContent()).isEmpty();

        tags.clear();
        clusters = this.service.getClusters(null, null, tags, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_1_ID);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetClustersByMinUpdateTime() {
        final ZonedDateTime time = ZonedDateTime.of(2014, Month.JULY.getValue(), 9, 2, 58, 59, 0, ZoneId.of("UTC"));
        final Page<Cluster> clusters = this.service.getClusters(null, null, null, time.toInstant(), null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetClustersByMaxUpdateTime() {
        final ZonedDateTime time = ZonedDateTime.of(2014, Month.JULY.getValue(), 8, 3, 0, 0, 0, ZoneId.of("UTC"));
        final Page<Cluster> clusters
            = this.service.getClusters(null, null, null, null, time.toInstant(), PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_1_ID);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetClustersDescending() {
        //Default to order by Updated
        final Page<Cluster> clusters = this.service.getClusters(null, null, null, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_1_ID);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetClustersAscending() {
        final Pageable ascendingPage = PageRequest.of(0, 10, Sort.Direction.ASC, "updated");
        //Default to order by Updated
        final Page<Cluster> clusters = this.service.getClusters(null, null, null, null, null, ascendingPage);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_1_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_2_ID);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetClustersOrderBysUser() {
        final Pageable userPage = PageRequest.of(0, 10, Sort.Direction.DESC, "user");
        final Page<Cluster> clusters = this.service.getClusters(null, null, null, null, null, userPage);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_1_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_2_ID);
    }

    @Test
    void testGetClustersOrderBysInvalidField() {
        final Pageable badPage = PageRequest.of(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
        Assertions
            .assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> this.service.getClusters(null, null, null, null, null, badPage));
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testChooseClusterForJob() throws GenieException {
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

        Assertions.assertThat(this.service.findClustersAndCommandsForJob(one)).hasSize(1);
        Assertions.assertThat(this.service.findClustersAndCommandsForJob(two)).isEmpty();
        Assertions.assertThat(this.service.findClustersAndCommandsForJob(three)).isEmpty();
        Assertions.assertThat(this.service.findClustersAndCommandsForJob(four)).hasSize(2);
        Assertions.assertThat(this.service.findClustersAndCommandsForJob(five)).hasSize(2);
    }

    @Test
    @SuppressWarnings("checkstyle:methodlength")
    // TODO: This would be much easier with a spock data test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testChooseClusterAndCommandForCriteria() throws GenieException {
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
        Assertions
            .assertThat(clustersAndCommands)
            .hasSize(1)
            .containsValue(COMMAND_1_ID)
            .extractingFromEntries(e -> e.getKey().getId())
            .containsOnly(CLUSTER_1_ID);

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
        Assertions.assertThat(clustersAndCommands).isEmpty();

        // Cluster is not UP so this should fail even though the id matches
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
        Assertions.assertThat(clustersAndCommands).isEmpty();

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
        Assertions
            .assertThat(clustersAndCommands)
            .hasSize(1)
            .containsValue(COMMAND_1_ID)
            .extractingFromEntries(e -> e.getKey().getId())
            .containsOnly(CLUSTER_2_ID);

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
        Assertions
            .assertThat(clustersAndCommands)
            .hasSize(2)
            .containsValue(COMMAND_3_ID)
            .doesNotContainValue(COMMAND_1_ID)
            .doesNotContainValue(COMMAND_2_ID)
            .extractingFromEntries(e -> e.getKey().getId())
            .containsExactlyInAnyOrder(CLUSTER_1_ID, CLUSTER_2_ID);

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
        Assertions
            .assertThat(clustersAndCommands)
            .hasSize(1)
            .containsValue(COMMAND_2_ID)
            .extractingFromEntries(e -> e.getKey().getId())
            .containsOnly(CLUSTER_2_ID);

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
        Assertions.assertThat(clustersAndCommands).hasSize(2);
        clustersAndCommands.forEach(
            (key, value) -> {
                switch (key.getId()) {
                    case CLUSTER_1_ID:
                        Assertions.assertThat(value).isEqualTo(COMMAND_1_ID);
                        break;
                    case CLUSTER_2_ID:
                        Assertions.assertThat(value).isEqualTo(COMMAND_4_ID);
                        break;
                    default:
                        Assertions.fail("Unknown cluster id " + key.getId());
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
        Assertions.assertThat(clustersAndCommands).isEmpty();

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
        Assertions
            .assertThat(clustersAndCommands)
            .hasSize(1)
            .containsValue(COMMAND_3_ID)
            .extractingFromEntries(e -> e.getKey().getId())
            .containsOnly(CLUSTER_1_ID);
    }

    @Test
    void testCreateCluster() throws GenieException {
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
        Assertions.assertThat(created.getId()).isEqualTo(id);
        final ClusterMetadata createdMetadata = created.getMetadata();
        Assertions.assertThat(createdMetadata.getName()).isEqualTo(CLUSTER_1_NAME);
        Assertions.assertThat(createdMetadata.getUser()).isEqualTo(CLUSTER_1_USER);
        Assertions.assertThat(createdMetadata.getStatus()).isEqualByComparingTo(ClusterStatus.OUT_OF_SERVICE);
        Assertions.assertThat(created.getResources().getConfigs()).isEqualTo(configs);
        Assertions.assertThat(created.getResources().getDependencies()).isEqualTo(dependencies);
        this.service.deleteCluster(id);
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getCluster(id));
    }

    @Test
    void testCreateClusterNoId() throws GenieException {
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
        final ClusterMetadata createdMetadata = created.getMetadata();
        Assertions.assertThat(createdMetadata.getName()).isEqualTo(CLUSTER_1_NAME);
        Assertions.assertThat(createdMetadata.getUser()).isEqualTo(CLUSTER_1_USER);
        Assertions.assertThat(createdMetadata.getStatus()).isEqualByComparingTo(ClusterStatus.OUT_OF_SERVICE);
        Assertions.assertThat(created.getResources().getConfigs()).isEqualTo(configs);
        Assertions.assertThat(created.getResources().getDependencies()).isEqualTo(dependencies);
        this.service.deleteCluster(id);
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getCluster(id));
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateClusterNoId() throws GenieException {
        final Cluster getCluster = this.service.getCluster(CLUSTER_1_ID);
        Assertions.assertThat(getCluster.getMetadata().getUser()).isEqualTo(CLUSTER_1_USER);
        Assertions.assertThat(getCluster.getMetadata().getStatus()).isEqualByComparingTo(ClusterStatus.UP);
        Assertions.assertThat(getCluster.getMetadata().getTags()).hasSize(3);

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
        Assertions.assertThat(updated.getMetadata().getUser()).isEqualTo(CLUSTER_2_USER);
        Assertions.assertThat(updated.getMetadata().getStatus()).isEqualByComparingTo(ClusterStatus.OUT_OF_SERVICE);
        Assertions.assertThat(updated.getMetadata().getTags()).hasSize(6);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateClusterWithInvalidCluster() throws GenieException {
        final Cluster getCluster = this.service.getCluster(CLUSTER_1_ID);
        Assertions.assertThat(getCluster.getMetadata().getUser()).isEqualTo(CLUSTER_1_USER);
        Assertions.assertThat(getCluster.getMetadata().getStatus()).isEqualByComparingTo(ClusterStatus.UP);
        Assertions.assertThat(getCluster.getMetadata().getTags()).hasSize(3);

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

        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.service.updateCluster(CLUSTER_1_ID, updateCluster));
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateCreateAndUpdate() throws GenieException {
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
        Assertions.assertThat(updatedCluster.getCreated()).isEqualTo(created);
        Assertions.assertThat(updatedCluster.getUpdated()).isNotEqualTo(updated).isNotEqualTo(Instant.EPOCH);
        Assertions.assertThat(updatedCluster.getMetadata().getTags()).isEqualTo(getCluster.getMetadata().getTags());
        Assertions
            .assertThat(updatedCluster.getResources().getConfigs())
            .isEqualTo(getCluster.getResources().getConfigs());
        Assertions
            .assertThat(updatedCluster.getResources().getDependencies())
            .isEqualTo(getCluster.getResources().getDependencies());
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testPatchCluster() throws GenieException, IOException {
        final Cluster getCluster = this.service.getCluster(CLUSTER_1_ID);
        Assertions.assertThat(getCluster.getMetadata().getName()).isEqualTo(CLUSTER_1_NAME);
        final Instant updateTime = getCluster.getUpdated();

        final String patchString
            = "[{ \"op\": \"replace\", \"path\": \"/metadata/name\", \"value\": \"" + CLUSTER_2_NAME + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));

        this.service.patchCluster(CLUSTER_1_ID, patch);

        final Cluster updated = this.service.getCluster(CLUSTER_1_ID);
        Assertions.assertThat(updated.getUpdated()).isNotEqualTo(updateTime);
        Assertions.assertThat(updated.getMetadata().getName()).isEqualTo(CLUSTER_2_NAME);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testDeleteAll() throws GenieException {
        Assertions
            .assertThat(this.service.getClusters(null, null, null, null, null, PAGE).getNumberOfElements())
            .isEqualTo(2);
        this.service.deleteAllClusters();
        Assertions
            .assertThat(this.service.getClusters(null, null, null, null, null, PAGE).getContent())
            .isEmpty();
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testDelete() throws GenieException {
        Assertions.assertThat(this.commandPersistenceService.getClustersForCommand(COMMAND_1_ID, null)).hasSize(2);
        Assertions.assertThat(this.commandPersistenceService.getClustersForCommand(COMMAND_2_ID, null)).hasSize(2);
        Assertions.assertThat(this.commandPersistenceService.getClustersForCommand(COMMAND_3_ID, null)).hasSize(2);

        this.service.deleteCluster(CLUSTER_1_ID);

        Assertions
            .assertThat(this.commandPersistenceService.getClustersForCommand(COMMAND_1_ID, null))
            .hasSize(1)
            .extracting(Cluster::getId)
            .containsOnly(CLUSTER_2_ID);
        Assertions
            .assertThat(this.commandPersistenceService.getClustersForCommand(COMMAND_2_ID, null))
            .hasSize(1)
            .extracting(Cluster::getId)
            .containsOnly(CLUSTER_2_ID);
        Assertions
            .assertThat(this.commandPersistenceService.getClustersForCommand(COMMAND_3_ID, null))
            .hasSize(1)
            .extracting(Cluster::getId)
            .containsOnly(CLUSTER_2_ID);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testAddConfigsToCluster() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assertions.assertThat(this.service.getConfigsForCluster(CLUSTER_1_ID)).hasSize(1);
        this.service.addConfigsForCluster(CLUSTER_1_ID, newConfigs);
        Assertions.assertThat(this.service.getConfigsForCluster(CLUSTER_1_ID)).hasSize(4).containsAll(newConfigs);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateConfigsForCluster() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assertions.assertThat(this.service.getConfigsForCluster(CLUSTER_1_ID)).hasSize(1);
        this.service.updateConfigsForCluster(CLUSTER_1_ID, newConfigs);
        Assertions.assertThat(this.service.getConfigsForCluster(CLUSTER_1_ID)).hasSize(3).containsAll(newConfigs);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetConfigsForCluster() throws GenieException {
        Assertions.assertThat(this.service.getConfigsForCluster(CLUSTER_1_ID)).hasSize(1);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testAddDependenciesToCluster() throws GenieException {
        final String newDep1 = UUID.randomUUID().toString();
        final String newDep2 = UUID.randomUUID().toString();
        final String newDep3 = UUID.randomUUID().toString();

        final Set<String> newDeps = Sets.newHashSet(newDep1, newDep2, newDep3);

        Assertions.assertThat(this.service.getDependenciesForCluster(CLUSTER_1_ID)).hasSize(2);
        this.service.addDependenciesForCluster(CLUSTER_1_ID, newDeps);
        Assertions.assertThat(this.service.getDependenciesForCluster(CLUSTER_1_ID)).hasSize(5).containsAll(newDeps);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateDependenciesForCluster() throws GenieException {
        final String newDep1 = UUID.randomUUID().toString();
        final String newDep2 = UUID.randomUUID().toString();
        final String newDep3 = UUID.randomUUID().toString();

        final Set<String> newDeps = Sets.newHashSet(newDep1, newDep2, newDep3);

        Assertions.assertThat(this.service.getDependenciesForCluster(CLUSTER_1_ID)).hasSize(2);
        this.service.updateDependenciesForCluster(CLUSTER_1_ID, newDeps);
        Assertions.assertThat(this.service.getDependenciesForCluster(CLUSTER_1_ID)).hasSize(3).containsAll(newDeps);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetDependenciesForCluster() throws GenieException {
        Assertions.assertThat(this.service.getDependenciesForCluster(CLUSTER_1_ID)).hasSize(2);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testAddCommandsForCluster() throws GenieException {
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
        final List<String> newCommandIds = Lists.newArrayList(command1Id, command2Id);
        Assertions.assertThat(this.service.getCommandsForCluster(CLUSTER_1_ID, null)).hasSize(3);
        this.service.addCommandsForCluster(CLUSTER_1_ID, newCommandIds);
        final List<Command> commands = this.service.getCommandsForCluster(CLUSTER_1_ID, null);
        Assertions.assertThat(commands).hasSize(5);
        Assertions.assertThat(commands.get(3).getId()).isEqualTo(command1Id);
        Assertions.assertThat(commands.get(4).getId()).isEqualTo(command2Id);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetCommandsForCluster() throws GenieException {
        Assertions
            .assertThat(this.service.getCommandsForCluster(CLUSTER_1_ID, null))
            .hasSize(3)
            .extracting(Command::getId)
            .containsExactly(COMMAND_1_ID, COMMAND_3_ID, COMMAND_2_ID);
    }

    @Test
    void testGetCommandsForClusterNoId() {
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.service.getCommandsForCluster("", null));
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateCommandsForCluster() throws GenieException {
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
        final List<String> newCommandIds = Lists.newArrayList(command1Id, command2Id);
        Assertions.assertThat(this.service.getCommandsForCluster(CLUSTER_1_ID, null)).hasSize(3);
        this.service.setCommandsForCluster(CLUSTER_1_ID, newCommandIds);
        Assertions
            .assertThat(this.service.getCommandsForCluster(CLUSTER_1_ID, null))
            .hasSize(2)
            .extracting(Command::getId)
            .containsExactly(command1Id, command2Id);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testRemoveAllCommandsForCluster() throws GenieException {
        Assertions.assertThat(this.service.getCommandsForCluster(CLUSTER_1_ID, null)).hasSize(3);
        this.service.removeAllCommandsForCluster(CLUSTER_1_ID);
        Assertions.assertThat(this.service.getCommandsForCluster(CLUSTER_1_ID, null)).isEmpty();
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testRemoveCommandForCluster() throws GenieException {
        Assertions.assertThat(this.service.getCommandsForCluster(CLUSTER_1_ID, null)).hasSize(3);
        this.service.removeCommandForCluster(CLUSTER_1_ID, COMMAND_1_ID);
        Assertions
            .assertThat(this.service.getCommandsForCluster(CLUSTER_1_ID, null))
            .hasSize(2)
            .extracting(Command::getId)
            .doesNotContain(COMMAND_1_ID);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testAddTagsToCluster() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assertions.assertThat(this.service.getTagsForCluster(CLUSTER_1_ID)).hasSize(3);
        this.service.addTagsForCluster(CLUSTER_1_ID, newTags);
        Assertions
            .assertThat(this.service.getTagsForCluster(CLUSTER_1_ID))
            .hasSize(6)
            .containsAll(newTags);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testUpdateTagsForCluster() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assertions.assertThat(this.service.getTagsForCluster(CLUSTER_1_ID)).hasSize(3);
        this.service.updateTagsForCluster(CLUSTER_1_ID, newTags);
        Assertions
            .assertThat(this.service.getTagsForCluster(CLUSTER_1_ID))
            .hasSize(3)
            .containsAll(newTags);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testGetTagsForCluster() throws GenieException {
        Assertions.assertThat(this.service.getTagsForCluster(CLUSTER_1_ID)).hasSize(3);
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testRemoveAllTagsForCluster() throws GenieException {
        Assertions.assertThat(this.service.getTagsForCluster(CLUSTER_1_ID)).hasSize(3);
        this.service.removeAllTagsForCluster(CLUSTER_1_ID);
        Assertions.assertThat(this.service.getTagsForCluster(CLUSTER_1_ID)).isEmpty();
    }

    /**
     * Test remove tag for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testRemoveTagForCluster() throws GenieException {
        Assertions.assertThat(this.service.getTagsForCluster(CLUSTER_1_ID)).contains("prod");
        this.service.removeTagForCluster(CLUSTER_1_ID, "prod");
        Assertions.assertThat(this.service.getTagsForCluster(CLUSTER_1_ID)).doesNotContain("prod");
    }

    @Test
    @DatabaseSetup("JpaClusterPersistenceServiceImplIntegrationTest/init.xml")
    void testDeleteTerminatedClusters() throws GenieException, IOException {
        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(2L);
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

        final Instant creationThreshold = Instant.now().plus(1L, ChronoUnit.SECONDS);
        final Set<ClusterStatus> deleteStatuses = EnumSet.of(ClusterStatus.TERMINATED);

        // Shouldn't delete any clusters as all are UP or OOS
        Assertions.assertThat(this.service.deleteUnusedClusters(deleteStatuses, creationThreshold)).isEqualTo(0);

        // Change status to UP
        String patchString = "[{ \"op\": \"replace\", \"path\": \"/metadata/status\", \"value\": \"UP\" }]";
        JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));
        this.service.patchCluster(testClusterId, patch);
        Assertions
            .assertThat(this.service.getCluster(testClusterId).getMetadata().getStatus())
            .isEqualTo(ClusterStatus.UP);

        // All clusters are UP/OOS or attached to jobs
        Assertions.assertThat(this.service.deleteUnusedClusters(deleteStatuses, creationThreshold)).isEqualTo(0);

        // Change status to terminated
        patchString = "[{ \"op\": \"replace\", \"path\": \"/metadata/status\", \"value\": \"TERMINATED\" }]";
        patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));
        this.service.patchCluster(testClusterId, patch);
        Assertions
            .assertThat(this.service.getCluster(testClusterId).getMetadata().getStatus())
            .isEqualTo(ClusterStatus.TERMINATED);

        // All clusters are UP/OOS or attached to jobs
        Assertions.assertThat(this.service.deleteUnusedClusters(deleteStatuses, creationThreshold)).isEqualTo(1);

        // Make sure it didn't delete any of the clusters we wanted
        Assertions.assertThat(this.clusterRepository.existsByUniqueId(CLUSTER_1_ID)).isTrue();
        Assertions.assertThat(this.clusterRepository.existsByUniqueId(CLUSTER_2_ID)).isTrue();
        Assertions.assertThat(this.clusterRepository.existsByUniqueId(testClusterId)).isFalse();
    }

    @Test
    void testFindClustersMatchingCriterion() throws Exception {
        // Create some clusters to test with
        final Cluster cluster0 = this.createTestCluster(null, null);
        final Cluster cluster1 = this.createTestCluster(null, null);
        final Cluster cluster2 = this.createTestCluster(UUID.randomUUID().toString(), null);

        // Create two commands with supersets of cluster1 tags so that we can test that resolution
        final Set<String> cluster3Tags = Sets.newHashSet(cluster1.getMetadata().getTags());
        cluster3Tags.add(UUID.randomUUID().toString());
        cluster3Tags.add(UUID.randomUUID().toString());
        final Cluster cluster3 = this.createTestCluster(null, cluster3Tags);
        final Set<String> cluster4Tags = Sets.newHashSet(cluster1.getMetadata().getTags());
        cluster4Tags.add(UUID.randomUUID().toString());
        final Cluster cluster4 = this.createTestCluster(null, cluster4Tags);

        Assertions
            .assertThat(
                this.service.findClustersMatchingCriterion(
                    new Criterion.Builder().withId(cluster0.getId()).build(), true
                )
            )
            .hasSize(1)
            .containsExactlyInAnyOrder(cluster0);

        Assertions
            .assertThat(
                this.service.findClustersMatchingCriterion(
                    new Criterion.Builder().withName(cluster2.getMetadata().getName()).build(),
                    true
                )
            )
            .hasSize(1)
            .containsExactlyInAnyOrder(cluster2);

        Assertions
            .assertThat(
                this.service.findClustersMatchingCriterion(
                    new Criterion.Builder().withVersion(cluster1.getMetadata().getVersion()).build(),
                    true
                )
            )
            .hasSize(1)
            .containsExactlyInAnyOrder(cluster1);

        // This comes from the init.xml
        Assertions
            .assertThat(
                this.service.findClustersMatchingCriterion(
                    new Criterion.Builder().withStatus(ClusterStatus.OUT_OF_SERVICE.name()).build(),
                    false
                )
            )
            .isEmpty();

        Assertions
            .assertThat(
                this.service.findClustersMatchingCriterion(
                    new Criterion.Builder().withTags(cluster1.getMetadata().getTags()).build(),
                    true
                )
            )
            .hasSize(3)
            .containsExactlyInAnyOrder(cluster1, cluster3, cluster4);

        Assertions
            .assertThat(
                this.service.findClustersMatchingCriterion(
                    new Criterion.Builder().withTags(cluster4.getMetadata().getTags()).build(),
                    true
                )
            )
            .hasSize(1)
            .containsExactlyInAnyOrder(cluster4);

        Assertions
            .assertThat(
                this.service.findClustersMatchingCriterion(
                    new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build(),
                    true
                )
            )
            .isEmpty();

        // Everything
        Assertions
            .assertThat(
                this.service.findClustersMatchingCriterion(
                    new Criterion.Builder()
                        .withId(cluster3.getId())
                        .withName(cluster3.getMetadata().getName())
                        .withVersion(cluster3.getMetadata().getVersion())
                        .withTags(cluster1.getMetadata().getTags()) // should be subset
                        .build(),
                    true
                )
            )
            .hasSize(1)
            .containsExactlyInAnyOrder(cluster3);
    }

    private Cluster createTestCluster(
        @Nullable final String id,
        @Nullable final Set<String> tags
    ) throws GenieException {
        final ClusterRequest.Builder requestBuilder = new ClusterRequest.Builder(
            new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.UP
            )
                .withTags(
                    tags == null ? Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()) : tags
                )
                .build()
        );

        if (id != null) {
            requestBuilder.withRequestedId(id);
        }

        final String clusterId = this.service.createCluster(requestBuilder.build());
        return this.service.getCluster(clusterId);
    }
}
