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
package com.netflix.genie.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.client.apis.SortAttribute;
import com.netflix.genie.client.apis.SortDirection;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for {@link ClusterClient}.
 *
 * @author amsharma
 */
abstract class ClusterClientIntegrationTest extends CommandClientIntegrationTest {


    @Test
    void testGetClustersUsingPagination() throws Exception {
        final String id1 = UUID.randomUUID().toString() + "_1";
        final String id2 = UUID.randomUUID().toString() + "_2";
        final String id3 = UUID.randomUUID().toString() + "_3";

        final List<String> ids = Lists.newArrayList(id1, id2, id3);

        for (final String id : ids) {
            final Cluster cluster = new Cluster.Builder(
                "ClusterName",
                "cluster-user",
                "1.0",
                ClusterStatus.UP
            )
                .withId(id)
                .build();

            this.clusterClient.createCluster(cluster);
        }

        final List<Cluster> results = this.clusterClient.getClusters(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        Assertions.assertThat(results).hasSize(3);
        Assertions.assertThat(
            results.stream()
                .map(Cluster::getId)
                .map(Optional::get)
        ).containsExactlyInAnyOrder(id1, id2, id3);

        // Paginate, 1 result per page
        for (int i = 0; i < ids.size(); i++) {
            final List<Cluster> page = this.clusterClient.getClusters(
                null,
                null,
                null,
                null,
                null,
                1,
                SortAttribute.UPDATED,
                SortDirection.ASC,
                i
            );

            Assertions.assertThat(page.size()).isEqualTo(1);
            Assertions.assertThat(page.get(0).getId().orElse(null)).isEqualTo(ids.get(i));
        }

        // Paginate, 1 result per page, reverse order
        Collections.reverse(ids);
        for (int i = 0; i < ids.size(); i++) {
            final List<Cluster> page = this.clusterClient.getClusters(
                null,
                null,
                null,
                null,
                null,
                1,
                SortAttribute.UPDATED,
                SortDirection.DESC,
                i
            );

            Assertions.assertThat(page.size()).isEqualTo(1);
            Assertions.assertThat(page.get(0).getId().orElse(null)).isEqualTo(ids.get(i));
        }

        // Ask for page beyond end of results
        Assertions.assertThat(
            this.clusterClient.getClusters(
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                1
            )
        ).isEmpty();
    }

    @Test
    void testUpdateCluster() throws Exception {
        final Cluster cluster1 = constructClusterDTO(null);
        final String cluster1Id = this.clusterClient.createCluster(cluster1);

        final Cluster cluster2 = this.clusterClient.getCluster(cluster1Id);
        Assertions.assertThat(cluster2.getName()).isEqualTo(cluster1.getName());

        final Cluster cluster3 = new Cluster.Builder(
            "newname",
            "newuser",
            "new version",
            ClusterStatus.OUT_OF_SERVICE
        )
            .withId(cluster1Id)
            .build();

        this.clusterClient.updateCluster(cluster1Id, cluster3);

        final Cluster cluster4 = this.clusterClient.getCluster(cluster1Id);

        Assertions.assertThat(cluster4.getName()).isEqualTo("newname");
        Assertions.assertThat(cluster4.getUser()).isEqualTo("newuser");
        Assertions.assertThat(cluster4.getVersion()).isEqualTo("new version");
        Assertions.assertThat(cluster4.getStatus()).isEqualByComparingTo(ClusterStatus.OUT_OF_SERVICE);
        Assertions.assertThat(cluster4.getSetupFile()).isNotPresent();
        Assertions.assertThat(cluster4.getDescription()).isNotPresent();
        Assertions.assertThat(cluster4.getConfigs()).isEmpty();
        Assertions.assertThat(cluster4.getTags()).doesNotContain("foo");
    }

    @Test
    void testClusterTagsMethods() throws Exception {
        final Set<String> initialTags = Sets.newHashSet("foo", "bar");

        final Cluster cluster = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withTags(initialTags)
            .build();

        final String clusterId = this.clusterClient.createCluster(cluster);

        // Test getTags for cluster
        Assertions.assertThat(this.clusterClient.getTagsForCluster(clusterId)).hasSize(4).contains("foo", "bar");

        // Test adding a tag for cluster
        final Set<String> moreTags = Sets.newHashSet("pi");

        this.clusterClient.addTagsToCluster(clusterId, moreTags);
        Assertions.assertThat(this.clusterClient.getTagsForCluster(clusterId)).hasSize(5).contains("foo", "bar", "pi");

        // Test removing a tag for cluster
        this.clusterClient.removeTagFromCluster(clusterId, "bar");
        Assertions.assertThat(this.clusterClient.getTagsForCluster(clusterId)).hasSize(4).contains("foo", "pi");

        // Test update tags for a cluster
        this.clusterClient.updateTagsForCluster(clusterId, initialTags);
        Assertions.assertThat(this.clusterClient.getTagsForCluster(clusterId)).hasSize(4).contains("foo", "bar");

        // Test delete all tags in a cluster
        this.clusterClient.removeAllTagsForCluster(clusterId);
        Assertions.assertThat(this.clusterClient.getTagsForCluster(clusterId)).hasSize(2).doesNotContain("foo", "bar");
    }

    @Test
    void testClusterConfigsMethods() throws Exception {
        final Set<String> initialConfigs = Sets.newHashSet("foo", "bar");

        final Cluster cluster = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withConfigs(initialConfigs)
            .build();

        final String clusterId = this.clusterClient.createCluster(cluster);

        // Test getConfigs for cluster
        Assertions.assertThat(this.clusterClient.getConfigsForCluster(clusterId)).hasSize(2).contains("foo", "bar");

        // Test adding a config for cluster
        final Set<String> moreConfigs = Sets.newHashSet("pi");

        this.clusterClient.addConfigsToCluster(clusterId, moreConfigs);
        Assertions
            .assertThat(this.clusterClient.getConfigsForCluster(clusterId))
            .hasSize(3)
            .contains("foo", "bar", "pi");

        // Test update configs for a cluster
        this.clusterClient.updateConfigsForCluster(clusterId, initialConfigs);
        Assertions.assertThat(this.clusterClient.getConfigsForCluster(clusterId)).hasSize(2).contains("foo", "bar");

        // Test delete all configs in a cluster
        this.clusterClient.removeAllConfigsForCluster(clusterId);
        Assertions.assertThat(this.clusterClient.getConfigsForCluster(clusterId)).isEmpty();
    }

    @Test
    void testClusterDependenciesMethods() throws Exception {
        final Set<String> initialDependencies = Sets.newHashSet("foo", "bar");

        final Cluster cluster = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withDependencies(initialDependencies)
            .build();

        final String clusterId = this.clusterClient.createCluster(cluster);

        // Test getDependencies for cluster
        Assertions
            .assertThat(this.clusterClient.getDependenciesForCluster(clusterId))
            .hasSize(2)
            .contains("foo", "bar");

        // Test adding a config for cluster
        final Set<String> moreDependencies = Sets.newHashSet("pi");

        this.clusterClient.addDependenciesToCluster(clusterId, moreDependencies);
        Assertions
            .assertThat(this.clusterClient.getDependenciesForCluster(clusterId))
            .hasSize(3)
            .contains("foo", "bar", "pi");

        // Test update configs for a cluster
        this.clusterClient.updateDependenciesForCluster(clusterId, initialDependencies);
        Assertions
            .assertThat(this.clusterClient.getDependenciesForCluster(clusterId))
            .hasSize(2)
            .contains("foo", "bar");

        // Test delete all configs in a cluster
        this.clusterClient.removeAllDependenciesForCluster(clusterId);
        Assertions.assertThat(this.clusterClient.getDependenciesForCluster(clusterId)).isEmpty();
    }

    @Test
    void testClusterCommandsMethods() throws Exception {
        final List<String> executableAndArgs = Lists.newArrayList("exec");

        final Command foo = new Command.Builder(
            "name",
            "user",
            "version",
            CommandStatus.ACTIVE,
            executableAndArgs,
            5
        ).build();

        final String fooId = this.commandClient.createCommand(foo);

        final Command bar = new Command.Builder(
            "name",
            "user",
            "version",
            CommandStatus.ACTIVE,
            executableAndArgs,
            5
        ).build();

        final String barId = this.commandClient.createCommand(bar);

        final Command pi = new Command.Builder(
            "name",
            "user",
            "version",
            CommandStatus.ACTIVE,
            executableAndArgs,
            5
        ).build();

        final String piId = this.commandClient.createCommand(pi);

        final Cluster cluster = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP).build();

        final String clusterId = this.clusterClient.createCluster(cluster);

        // Test add Commands to cluster
        final List<String> initialCommands = Lists.newArrayList(fooId, barId, piId);

        // These are all no-ops now so just make sure they don't throw exceptions
        this.clusterClient.addCommandsToCluster(clusterId, initialCommands);
        Assertions.assertThat(this.clusterClient.getCommandsForCluster(clusterId)).isEmpty();
        this.clusterClient.removeCommandFromCluster(clusterId, barId);
        Assertions.assertThat(this.clusterClient.getCommandsForCluster(clusterId)).isEmpty();
        this.clusterClient.updateCommandsForCluster(clusterId, Lists.newArrayList(barId, fooId));
        Assertions.assertThat(this.clusterClient.getCommandsForCluster(clusterId)).isEmpty();
        this.clusterClient.removeAllCommandsForCluster(clusterId);
    }

    @Test
    void testClusterPatchMethod() throws Exception {
        final ObjectMapper mapper = GenieObjectMapper.getMapper();
        final String newName = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + newName + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(mapper.readTree(patchString));

        final Cluster cluster = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP).build();

        final String clusterId = this.clusterClient.createCluster(cluster);
        this.clusterClient.patchCluster(clusterId, patch);

        Assertions.assertThat(this.clusterClient.getCluster(clusterId).getName()).isEqualTo(newName);
    }
}
