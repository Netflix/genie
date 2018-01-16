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

import java.util.Set;
import java.util.UUID;

/**
 * Tests for ClusterCriteria.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(UnitTest.class)
public class ClusterCriteriaUnitTests {

    /**
     * Make sure the constructor create sets properly.
     */
    @Test
    public void canConstruct() {
        final Set<String> tags = Sets.newHashSet("tag1", "tag2");
        final ClusterCriteria cc = new ClusterCriteria(tags);
        Assert.assertThat(cc.getTags(), Matchers.is(tags));
    }

    /**
     * Test to make sure clients can't modify the internal state.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void cantModifyTags() {
        final Set<String> tags = Sets.newHashSet("tag1", "tag2");
        final ClusterCriteria cc = new ClusterCriteria(tags);
        cc.getTags().add("this should fail");
    }

    /**
     * Make sure we can use equals.
     */
    @Test
    public void canGetEquality() {
        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final ClusterCriteria clusterCriteria1 = new ClusterCriteria(tags);
        final ClusterCriteria clusterCriteria2 = new ClusterCriteria(tags);

        tags.add(UUID.randomUUID().toString());
        final ClusterCriteria clusterCriteria3 = new ClusterCriteria(tags);

        Assert.assertTrue(clusterCriteria1.equals(clusterCriteria2));
        Assert.assertFalse(clusterCriteria1.equals(clusterCriteria3));
    }

    /**
     * Make sure we can use equals.
     */
    @Test
    public void canGetHashCode() {
        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final ClusterCriteria clusterCriteria1 = new ClusterCriteria(tags);
        final ClusterCriteria clusterCriteria2 = new ClusterCriteria(tags);

        tags.add(UUID.randomUUID().toString());
        final ClusterCriteria clusterCriteria3 = new ClusterCriteria(tags);

        Assert.assertEquals(clusterCriteria1.hashCode(), clusterCriteria2.hashCode());
        Assert.assertNotEquals(clusterCriteria1.hashCode(), clusterCriteria3.hashCode());
    }
}
