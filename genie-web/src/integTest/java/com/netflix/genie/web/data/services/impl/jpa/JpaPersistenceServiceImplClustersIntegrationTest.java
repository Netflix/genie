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
package com.netflix.genie.web.data.services.impl.jpa;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
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
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
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
 * Integration tests for {@link JpaPersistenceServiceImpl} clusters specific functionality.
 *
 * @author tgianos
 * @since 2.0.0
 */
class JpaPersistenceServiceImplClustersIntegrationTest extends JpaPersistenceServiceIntegrationTestBase {

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

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetCluster() throws NotFoundException {
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
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetClustersByName() {
        final Page<Cluster> clusters = this.service.findClusters(CLUSTER_2_NAME, null, null, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetClustersByStatuses() {
        final Set<ClusterStatus> statuses = EnumSet.of(ClusterStatus.UP);
        final Page<Cluster> clusters = this.service.findClusters(null, statuses, null, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_1_ID);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetClustersByTags() {
        final Set<String> tags = Sets.newHashSet("prod");
        Page<Cluster> clusters = this.service.findClusters(null, null, tags, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_1_ID);

        tags.clear();
        tags.add("hive");
        clusters = this.service.findClusters(null, null, tags, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_1_ID);

        tags.add("somethingThatWouldNeverReallyExist");
        clusters = this.service.findClusters(null, null, tags, null, null, PAGE);
        Assertions.assertThat(clusters.getContent()).isEmpty();

        tags.clear();
        clusters = this.service.findClusters(null, null, tags, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_1_ID);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetClustersByMinUpdateTime() {
        final ZonedDateTime time = ZonedDateTime.of(2014, Month.JULY.getValue(), 9, 2, 58, 59, 0, ZoneId.of("UTC"));
        final Page<Cluster> clusters = this.service.findClusters(null, null, null, time.toInstant(), null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetClustersByMaxUpdateTime() {
        final ZonedDateTime time = ZonedDateTime.of(2014, Month.JULY.getValue(), 8, 3, 0, 0, 0, ZoneId.of("UTC"));
        final Page<Cluster> clusters = this.service.findClusters(null, null, null, null, time.toInstant(), PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_1_ID);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetClustersDescending() {
        //Default to order by Updated
        final Page<Cluster> clusters = this.service.findClusters(null, null, null, null, null, PAGE);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_2_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_1_ID);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetClustersAscending() {
        final Pageable ascendingPage = PageRequest.of(0, 10, Sort.Direction.ASC, "updated");
        //Default to order by Updated
        final Page<Cluster> clusters = this.service.findClusters(null, null, null, null, null, ascendingPage);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_1_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_2_ID);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetClustersOrderBysUser() {
        final Pageable userPage = PageRequest.of(0, 10, Sort.Direction.DESC, "user");
        final Page<Cluster> clusters = this.service.findClusters(null, null, null, null, null, userPage);
        Assertions.assertThat(clusters.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(clusters.getContent().get(0).getId()).isEqualTo(CLUSTER_1_ID);
        Assertions.assertThat(clusters.getContent().get(1).getId()).isEqualTo(CLUSTER_2_ID);
    }

    @Test
    void testGetClustersOrderBysInvalidField() {
        final Pageable badPage = PageRequest.of(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
        Assertions
            .assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> this.service.findClusters(null, null, null, null, null, badPage));
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
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
    @DatabaseSetup("persistence/clusters/init.xml")
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
    void testCreateCluster() throws GenieCheckedException {
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
        this.service.saveCluster(cluster);
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
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getCluster(id));
    }

    @Test
    void testCreateClusterNoId() throws GenieCheckedException {
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
        final String id = this.service.saveCluster(cluster);
        final Cluster created = this.service.getCluster(id);
        final ClusterMetadata createdMetadata = created.getMetadata();
        Assertions.assertThat(createdMetadata.getName()).isEqualTo(CLUSTER_1_NAME);
        Assertions.assertThat(createdMetadata.getUser()).isEqualTo(CLUSTER_1_USER);
        Assertions.assertThat(createdMetadata.getStatus()).isEqualByComparingTo(ClusterStatus.OUT_OF_SERVICE);
        Assertions.assertThat(created.getResources().getConfigs()).isEqualTo(configs);
        Assertions.assertThat(created.getResources().getDependencies()).isEqualTo(dependencies);
        this.service.deleteCluster(id);
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getCluster(id));
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testUpdateClusterNoId() throws GenieCheckedException {
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
    @DatabaseSetup("persistence/clusters/init.xml")
    void testUpdateClusterWithInvalidCluster() throws GenieCheckedException {
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
    @DatabaseSetup("persistence/clusters/init.xml")
    void testUpdateCreateAndUpdate() throws GenieCheckedException {
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
    @DatabaseSetup("persistence/clusters/init.xml")
    void testDeleteAll() throws GenieCheckedException {
        Assertions
            .assertThat(this.service.findClusters(null, null, null, null, null, PAGE).getNumberOfElements())
            .isEqualTo(2);
        this.service.deleteAllClusters();
        Assertions
            .assertThat(this.service.findClusters(null, null, null, null, null, PAGE).getContent())
            .isEmpty();
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testDelete() throws GenieCheckedException {
        final CommandEntity command1 = this.commandRepository
            .findByUniqueId(COMMAND_1_ID)
            .orElseThrow(IllegalStateException::new);
        final CommandEntity command2 = this.commandRepository
            .findByUniqueId(COMMAND_1_ID)
            .orElseThrow(IllegalStateException::new);
        final CommandEntity command3 = this.commandRepository
            .findByUniqueId(COMMAND_1_ID)
            .orElseThrow(IllegalStateException::new);

        Assertions.assertThat(command1.getClusters()).hasSize(2);
        Assertions.assertThat(command2.getClusters()).hasSize(2);
        Assertions.assertThat(command3.getClusters()).hasSize(2);

        this.service.deleteCluster(CLUSTER_1_ID);

        Assertions.assertThat(command1.getClusters()).extracting(ClusterEntity::getUniqueId).containsOnly(CLUSTER_2_ID);
        Assertions.assertThat(command2.getClusters()).extracting(ClusterEntity::getUniqueId).containsOnly(CLUSTER_2_ID);
        Assertions.assertThat(command3.getClusters()).extracting(ClusterEntity::getUniqueId).containsOnly(CLUSTER_2_ID);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testAddConfigsToCluster() throws GenieCheckedException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assertions.assertThat(this.service.getConfigsForResource(CLUSTER_1_ID, Cluster.class)).hasSize(1);
        this.service.addConfigsToResource(CLUSTER_1_ID, newConfigs, Cluster.class);
        Assertions.assertThat(this.service.getConfigsForResource(CLUSTER_1_ID, Cluster.class))
            .hasSize(4)
            .containsAll(newConfigs);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testUpdateConfigsForCluster() throws GenieCheckedException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assertions.assertThat(this.service.getConfigsForResource(CLUSTER_1_ID, Cluster.class)).hasSize(1);
        this.service.updateConfigsForResource(CLUSTER_1_ID, newConfigs, Cluster.class);
        Assertions.assertThat(this.service.getConfigsForResource(CLUSTER_1_ID, Cluster.class))
            .hasSize(3)
            .containsAll(newConfigs);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetConfigsForCluster() throws GenieCheckedException {
        Assertions.assertThat(this.service.getConfigsForResource(CLUSTER_1_ID, Cluster.class)).hasSize(1);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testAddDependenciesToCluster() throws GenieCheckedException {
        final String newDep1 = UUID.randomUUID().toString();
        final String newDep2 = UUID.randomUUID().toString();
        final String newDep3 = UUID.randomUUID().toString();

        final Set<String> newDeps = Sets.newHashSet(newDep1, newDep2, newDep3);

        Assertions.assertThat(this.service.getDependenciesForResource(CLUSTER_1_ID, Cluster.class)).hasSize(2);
        this.service.addDependenciesToResource(CLUSTER_1_ID, newDeps, Cluster.class);
        Assertions.assertThat(this.service.getDependenciesForResource(CLUSTER_1_ID, Cluster.class))
            .hasSize(5)
            .containsAll(newDeps);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testUpdateDependenciesForCluster() throws GenieCheckedException {
        final String newDep1 = UUID.randomUUID().toString();
        final String newDep2 = UUID.randomUUID().toString();
        final String newDep3 = UUID.randomUUID().toString();

        final Set<String> newDeps = Sets.newHashSet(newDep1, newDep2, newDep3);

        Assertions.assertThat(this.service.getDependenciesForResource(CLUSTER_1_ID, Cluster.class)).hasSize(2);
        this.service.updateDependenciesForResource(CLUSTER_1_ID, newDeps, Cluster.class);
        Assertions.assertThat(this.service.getDependenciesForResource(CLUSTER_1_ID, Cluster.class))
            .hasSize(3)
            .containsAll(newDeps);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetDependenciesForCluster() throws GenieCheckedException {
        Assertions.assertThat(this.service.getDependenciesForResource(CLUSTER_1_ID, Cluster.class)).hasSize(2);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testAddCommandsForCluster() throws GenieCheckedException, GenieException {
        final String command1Id = this.service.saveCommand(
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
        final String command2Id = this.service.saveCommand(
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
    @DatabaseSetup("persistence/clusters/init.xml")
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
    @DatabaseSetup("persistence/clusters/init.xml")
    void testUpdateCommandsForCluster() throws GenieCheckedException, GenieException {
        final String command1Id = this.service.saveCommand(
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
        final String command2Id = this.service.saveCommand(
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
    @DatabaseSetup("persistence/clusters/init.xml")
    void testRemoveAllCommandsForCluster() throws GenieException {
        Assertions.assertThat(this.service.getCommandsForCluster(CLUSTER_1_ID, null)).hasSize(3);
        this.service.removeAllCommandsForCluster(CLUSTER_1_ID);
        Assertions.assertThat(this.service.getCommandsForCluster(CLUSTER_1_ID, null)).isEmpty();
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
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
    @DatabaseSetup("persistence/clusters/init.xml")
    void testAddTagsToCluster() throws GenieCheckedException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assertions.assertThat(this.service.getTagsForResource(CLUSTER_1_ID, Cluster.class)).hasSize(3);
        this.service.addTagsToResource(CLUSTER_1_ID, newTags, Cluster.class);
        Assertions
            .assertThat(this.service.getTagsForResource(CLUSTER_1_ID, Cluster.class))
            .hasSize(6)
            .containsAll(newTags);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testUpdateTagsForCluster() throws GenieCheckedException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assertions.assertThat(this.service.getTagsForResource(CLUSTER_1_ID, Cluster.class)).hasSize(3);
        this.service.updateTagsForResource(CLUSTER_1_ID, newTags, Cluster.class);
        Assertions
            .assertThat(this.service.getTagsForResource(CLUSTER_1_ID, Cluster.class))
            .hasSize(3)
            .containsAll(newTags);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testGetTagsForCluster() throws GenieCheckedException {
        Assertions.assertThat(this.service.getTagsForResource(CLUSTER_1_ID, Cluster.class)).hasSize(3);
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testRemoveAllTagsForCluster() throws GenieCheckedException {
        Assertions.assertThat(this.service.getTagsForResource(CLUSTER_1_ID, Cluster.class)).hasSize(3);
        this.service.removeAllTagsForResource(CLUSTER_1_ID, Cluster.class);
        Assertions.assertThat(this.service.getTagsForResource(CLUSTER_1_ID, Cluster.class)).isEmpty();
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testRemoveTagForCluster() throws GenieCheckedException {
        Assertions.assertThat(this.service.getTagsForResource(CLUSTER_1_ID, Cluster.class)).contains("prod");
        this.service.removeTagForResource(CLUSTER_1_ID, "prod", Cluster.class);
        Assertions.assertThat(this.service.getTagsForResource(CLUSTER_1_ID, Cluster.class)).doesNotContain("prod");
    }

    @Test
    @DatabaseSetup("persistence/clusters/init.xml")
    void testDeleteUnusedClusters() throws GenieCheckedException {
        Assertions.assertThat(this.clusterRepository.count()).isEqualTo(2L);
        final String testCluster0Id = this.createTestCluster(null, null, ClusterStatus.OUT_OF_SERVICE).getId();

        final Instant creationThreshold = Instant.now().plus(10L, ChronoUnit.MINUTES);
        final Set<ClusterStatus> deleteStatuses = EnumSet.of(ClusterStatus.TERMINATED);

        // Shouldn't delete any clusters as all are UP or OOS
        Assertions.assertThat(this.service.deleteUnusedClusters(deleteStatuses, creationThreshold)).isEqualTo(0);

        // Add new up cluster
        final String testCluster1Id = this.createTestCluster(null, null, ClusterStatus.UP).getId();

        // All clusters are UP/OOS or attached to jobs
        Assertions.assertThat(this.service.deleteUnusedClusters(deleteStatuses, creationThreshold)).isEqualTo(0);

        // Create Terminated Cluster
        final String testCluster2Id = this.createTestCluster(null, null, ClusterStatus.TERMINATED).getId();

        // All clusters are UP/OOS or attached to jobs
        Assertions.assertThat(this.service.deleteUnusedClusters(deleteStatuses, creationThreshold)).isEqualTo(1);

        // Make sure it didn't delete any of the clusters we wanted
        Assertions.assertThat(this.clusterRepository.existsByUniqueId(CLUSTER_1_ID)).isTrue();
        Assertions.assertThat(this.clusterRepository.existsByUniqueId(CLUSTER_2_ID)).isTrue();
        Assertions.assertThat(this.clusterRepository.existsByUniqueId(testCluster0Id)).isTrue();
        Assertions.assertThat(this.clusterRepository.existsByUniqueId(testCluster1Id)).isTrue();
        Assertions.assertThat(this.clusterRepository.existsByUniqueId(testCluster2Id)).isFalse();
    }

    @Test
    void testFindClustersMatchingCriterion() throws Exception {
        // Create some clusters to test with
        final Cluster cluster0 = this.createTestCluster(null, null, null);
        final Cluster cluster1 = this.createTestCluster(null, null, null);
        final Cluster cluster2 = this.createTestCluster(UUID.randomUUID().toString(), null, null);

        // Create two commands with supersets of cluster1 tags so that we can test that resolution
        final Set<String> cluster3Tags = Sets.newHashSet(cluster1.getMetadata().getTags());
        cluster3Tags.add(UUID.randomUUID().toString());
        cluster3Tags.add(UUID.randomUUID().toString());
        final Cluster cluster3 = this.createTestCluster(null, cluster3Tags, null);
        final Set<String> cluster4Tags = Sets.newHashSet(cluster1.getMetadata().getTags());
        cluster4Tags.add(UUID.randomUUID().toString());
        final Cluster cluster4 = this.createTestCluster(null, cluster4Tags, null);

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
        @Nullable final Set<String> tags,
        @Nullable final ClusterStatus status
    ) throws GenieCheckedException {
        final ClusterRequest.Builder requestBuilder = new ClusterRequest.Builder(
            new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                status == null ? ClusterStatus.UP : status
            )
                .withTags(
                    tags == null ? Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()) : tags
                )
                .build()
        );

        if (id != null) {
            requestBuilder.withRequestedId(id);
        }

        final String clusterId = this.service.saveCluster(requestBuilder.build());
        return this.service.getCluster(clusterId);
    }
}
