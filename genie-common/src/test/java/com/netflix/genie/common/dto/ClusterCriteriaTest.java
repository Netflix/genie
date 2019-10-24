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
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

/**
 * Tests for ClusterCriteria.
 *
 * @author tgianos
 * @since 2.0.0
 */
class ClusterCriteriaTest {

    /**
     * Make sure the constructor create sets properly.
     */
    @Test
    void canConstruct() {
        final Set<String> tags = Sets.newHashSet("tag1", "tag2");
        final ClusterCriteria cc = new ClusterCriteria(tags);
        Assertions.assertThat(cc.getTags()).isEqualTo(tags);
    }

    /**
     * Test to make sure clients can't modify the internal state.
     */
    @Test
    void cantModifyTags() {
        final Set<String> tags = Sets.newHashSet("tag1", "tag2");
        final ClusterCriteria cc = new ClusterCriteria(tags);
        Assertions
            .assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> cc.getTags().add("this should fail"));
    }

    /**
     * Make sure we can use equals.
     */
    @Test
    void canGetEquality() {
        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final ClusterCriteria clusterCriteria1 = new ClusterCriteria(tags);
        final ClusterCriteria clusterCriteria2 = new ClusterCriteria(tags);

        tags.add(UUID.randomUUID().toString());
        final ClusterCriteria clusterCriteria3 = new ClusterCriteria(tags);

        Assert.assertEquals(clusterCriteria1, clusterCriteria2);
        Assert.assertNotEquals(clusterCriteria1, clusterCriteria3);
    }

    /**
     * Make sure we can use equals.
     */
    @Test
    void canGetHashCode() {
        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final ClusterCriteria clusterCriteria1 = new ClusterCriteria(tags);
        final ClusterCriteria clusterCriteria2 = new ClusterCriteria(tags);

        tags.add(UUID.randomUUID().toString());
        final ClusterCriteria clusterCriteria3 = new ClusterCriteria(tags);

        Assert.assertEquals(clusterCriteria1.hashCode(), clusterCriteria2.hashCode());
        Assert.assertNotEquals(clusterCriteria1.hashCode(), clusterCriteria3.hashCode());
    }
}
