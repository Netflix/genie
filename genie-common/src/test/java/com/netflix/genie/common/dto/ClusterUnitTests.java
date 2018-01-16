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
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the Cluster DTO.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class ClusterUnitTests {

    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();

    /**
     * Test to make sure we can build a cluster using the default builder constructor.
     */
    @Test
    public void canBuildCluster() {
        final Cluster cluster = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).build();
        Assert.assertThat(cluster.getName(), Matchers.is(NAME));
        Assert.assertThat(cluster.getUser(), Matchers.is(USER));
        Assert.assertThat(cluster.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(cluster.getStatus(), Matchers.is(ClusterStatus.UP));
        Assert.assertThat(cluster.getConfigs(), Matchers.empty());
        Assert.assertThat(cluster.getDependencies(), Matchers.empty());
        Assert.assertFalse(cluster.getCreated().isPresent());
        Assert.assertFalse(cluster.getDescription().isPresent());
        Assert.assertFalse(cluster.getSetupFile().isPresent());
        Assert.assertFalse(cluster.getId().isPresent());
        Assert.assertThat(cluster.getTags(), Matchers.empty());
        Assert.assertFalse(cluster.getUpdated().isPresent());
    }

    /**
     * Test to make sure we can build a cluster with all optional parameters.
     */
    @Test
    public void canBuildClusterWithOptionals() {
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
        Assert.assertThat(cluster.getName(), Matchers.is(NAME));
        Assert.assertThat(cluster.getUser(), Matchers.is(USER));
        Assert.assertThat(cluster.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(cluster.getStatus(), Matchers.is(ClusterStatus.UP));
        Assert.assertThat(cluster.getConfigs(), Matchers.is(configs));
        Assert.assertThat(cluster.getDependencies(), Matchers.is(dependencies));
        Assert.assertThat(cluster.getCreated().orElseThrow(IllegalArgumentException::new), Matchers.is(created));
        Assert.assertThat(
            cluster.getDescription().orElseThrow(IllegalArgumentException::new), Matchers.is(description)
        );
        Assert.assertThat(cluster.getId().orElseThrow(IllegalArgumentException::new), Matchers.is(id));
        Assert.assertThat(cluster.getTags(), Matchers.is(tags));
        Assert.assertThat(cluster.getUpdated().orElseThrow(IllegalArgumentException::new), Matchers.is(updated));
    }

    /**
     * Test to make sure we can build a cluster with null collection parameters.
     */
    @Test
    public void canBuildClusterNullOptionals() {
        final Cluster.Builder builder = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP);
        builder.withConfigs(null);
        builder.withDependencies(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(null);
        builder.withTags(null);
        builder.withUpdated(null);

        final Cluster cluster = builder.build();
        Assert.assertThat(cluster.getName(), Matchers.is(NAME));
        Assert.assertThat(cluster.getUser(), Matchers.is(USER));
        Assert.assertThat(cluster.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(cluster.getStatus(), Matchers.is(ClusterStatus.UP));
        Assert.assertThat(cluster.getConfigs(), Matchers.empty());
        Assert.assertThat(cluster.getDependencies(), Matchers.empty());
        Assert.assertFalse(cluster.getCreated().isPresent());
        Assert.assertFalse(cluster.getDescription().isPresent());
        Assert.assertFalse(cluster.getSetupFile().isPresent());
        Assert.assertFalse(cluster.getId().isPresent());
        Assert.assertThat(cluster.getTags(), Matchers.empty());
        Assert.assertFalse(cluster.getUpdated().isPresent());
    }

    /**
     * Test equals.
     */
    @Test
    public void canFindEquality() {
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

        Assert.assertTrue(cluster1.equals(cluster2));
        Assert.assertTrue(cluster2.equals(cluster1));
        Assert.assertTrue(cluster1.equals(cluster3));
        Assert.assertFalse(cluster1.equals(cluster4));
    }

    /**
     * Test hash code.
     */
    @Test
    public void canUseHashCode() {
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

        Assert.assertEquals(cluster1.hashCode(), cluster2.hashCode());
        Assert.assertNotEquals(cluster1.hashCode(), cluster3.hashCode());
    }
}
