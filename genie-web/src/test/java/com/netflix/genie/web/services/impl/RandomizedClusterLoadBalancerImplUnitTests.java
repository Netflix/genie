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
package com.netflix.genie.web.services.impl;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test for the cluster load balancer.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
public class RandomizedClusterLoadBalancerImplUnitTests {

    private RandomizedClusterLoadBalancerImpl clb;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.clb = new RandomizedClusterLoadBalancerImpl();
    }

    /**
     * Test whether a cluster is returned from a set of candidates.
     *
     * @throws GenieException For any problem if anything went wrong with the test.
     */
    @Test
    public void testValidClusterList() throws GenieException {
        final Cluster cluster1 = Mockito.mock(Cluster.class);
        final Cluster cluster2 = Mockito.mock(Cluster.class);
        final Cluster cluster3 = Mockito.mock(Cluster.class);
        Assert.assertNotNull(
            this.clb.selectCluster(
                Sets.newHashSet(cluster1, cluster2, cluster3),
                Mockito.mock(JobRequest.class)
            )
        );
    }

    /**
     * Test whether a cluster is returned from a set of candidates.
     *
     * @throws GenieException For any problem if anything went wrong with the test.
     */
    @Test
    public void testValidClusterListOfOne() throws GenieException {
        final Cluster cluster1 = Mockito.mock(Cluster.class);
        Assert.assertEquals(
            cluster1,
            this.clb.selectCluster(
                Sets.newHashSet(cluster1),
                Mockito.mock(JobRequest.class)
            )
        );
    }
}
