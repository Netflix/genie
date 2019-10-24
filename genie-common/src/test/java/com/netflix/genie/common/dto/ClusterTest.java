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
package com.netflix.genie.common.dto;

import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the {@link Cluster} DTO.
 *
 * @author tgianos
 * @since 3.0.0
 */
class ClusterTest {

    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();

    /**
     * Test to make sure we can build a cluster using the default builder constructor.
     */
    @Test
    void canBuildCluster() {
        final Cluster cluster = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).build();
        Assertions.assertThat(cluster.getName()).isEqualTo(NAME);
        Assertions.assertThat(cluster.getUser()).isEqualTo(USER);
        Assertions.assertThat(cluster.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(cluster.getStatus()).isEqualTo(ClusterStatus.UP);
        Assertions.assertThat(cluster.getConfigs()).isEmpty();
        Assertions.assertThat(cluster.getDependencies()).isEmpty();
        Assertions.assertThat(cluster.getCreated().isPresent()).isFalse();
        Assertions.assertThat(cluster.getDescription().isPresent()).isFalse();
        Assertions.assertThat(cluster.getSetupFile().isPresent()).isFalse();
        Assertions.assertThat(cluster.getId().isPresent()).isFalse();
        Assertions.assertThat(cluster.getTags()).isEmpty();
        Assertions.assertThat(cluster.getUpdated().isPresent()).isFalse();
    }

    /**
     * Test to make sure we can build a cluster with all optional parameters.
     */
    @Test
    void canBuildClusterWithOptionals() {
        final Cluster.Builder builder = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP);

        final Set<String> configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withConfigs(configs);

        final Set<String> dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withDependencies(dependencies);

        final Instant created = Instant.now();
        builder.withCreated(created);

        final String description = UUID.randomUUID().toString();
        builder.withDescription(description);

        final String id = UUID.randomUUID().toString();
        builder.withId(id);

        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withTags(tags);

        final Instant updated = Instant.now();
        builder.withUpdated(updated);

        final Cluster cluster = builder.build();
        Assertions.assertThat(cluster.getName()).isEqualTo(NAME);
        Assertions.assertThat(cluster.getUser()).isEqualTo(USER);
        Assertions.assertThat(cluster.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(cluster.getStatus()).isEqualTo(ClusterStatus.UP);
        Assertions.assertThat(cluster.getConfigs()).isEqualTo(configs);
        Assertions.assertThat(cluster.getDependencies()).isEqualTo(dependencies);
        Assertions.assertThat(cluster.getCreated().orElseThrow(IllegalArgumentException::new)).isEqualTo(created);
        Assertions
            .assertThat(cluster.getDescription().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(description);
        Assertions
            .assertThat(cluster.getId().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(id);
        Assertions.assertThat(cluster.getTags()).isEqualTo(tags);
        Assertions.assertThat(cluster.getUpdated().orElseThrow(IllegalArgumentException::new)).isEqualTo(updated);
    }

    /**
     * Test to make sure we can build a cluster with null collection parameters.
     */
    @Test
    void canBuildClusterNullOptionals() {
        final Cluster.Builder builder = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP);
        builder.withConfigs(null);
        builder.withDependencies(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(null);
        builder.withTags(null);
        builder.withUpdated(null);

        final Cluster cluster = builder.build();
        Assertions.assertThat(cluster.getName()).isEqualTo(NAME);
        Assertions.assertThat(cluster.getUser()).isEqualTo(USER);
        Assertions.assertThat(cluster.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(cluster.getStatus()).isEqualTo(ClusterStatus.UP);
        Assertions.assertThat(cluster.getConfigs()).isEmpty();
        Assertions.assertThat(cluster.getDependencies()).isEmpty();
        Assertions.assertThat(cluster.getCreated().isPresent()).isFalse();
        Assertions.assertThat(cluster.getDescription().isPresent()).isFalse();
        Assertions.assertThat(cluster.getSetupFile().isPresent()).isFalse();
        Assertions.assertThat(cluster.getId().isPresent()).isFalse();
        Assertions.assertThat(cluster.getTags()).isEmpty();
        Assertions.assertThat(cluster.getUpdated().isPresent()).isFalse();
    }

    /**
     * Test equals.
     */
    @Test
    void canFindEquality() {
        final Cluster.Builder builder = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP);
        builder.withConfigs(null);
        builder.withDependencies(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);
        final Cluster cluster1 = builder.build();
        final Cluster cluster2 = builder.build();
        builder.withDescription(UUID.randomUUID().toString());
        final Cluster cluster3 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final Cluster cluster4 = builder.build();

        Assertions.assertThat(cluster1).isEqualTo(cluster2);
        Assertions.assertThat(cluster1).isEqualTo(cluster3);
        Assertions.assertThat(cluster1).isNotEqualTo(cluster4);
    }

    /**
     * Test hash code.
     */
    @Test
    void canUseHashCode() {
        final Cluster.Builder builder = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.TERMINATED);
        builder.withConfigs(null);
        builder.withDependencies(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);
        final Cluster cluster1 = builder.build();
        final Cluster cluster2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final Cluster cluster3 = builder.build();

        Assertions.assertThat(cluster1.hashCode()).isEqualTo(cluster2.hashCode());
        Assertions.assertThat(cluster1.hashCode()).isNotEqualTo(cluster3.hashCode());
    }
}
