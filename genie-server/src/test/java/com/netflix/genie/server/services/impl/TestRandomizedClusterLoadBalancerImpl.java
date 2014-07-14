/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterStatus;
import com.netflix.genie.server.services.ClusterLoadBalancer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test for the cluster load balancer.
 *
 * @author skrishnan
 * @author tgianos
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:genie-application-test.xml")
public class TestRandomizedClusterLoadBalancerImpl {

    @Inject
    private ClusterLoadBalancer clb;

    /**
     * Test whether a cluster is returned from a set of candidates.
     *
     * @throws GenieException if anything went wrong with the test.
     */
    @Test
    public void testValidCluster() throws GenieException {
        final Set<String> configs = new HashSet<String>();
        configs.add("SomeConfig");
        final Cluster cce = new Cluster("name", "tgianos", ClusterStatus.UP, "jobManager", configs);
        assertNotNull(this.clb.selectCluster(Arrays.asList(new Cluster[]{cce, cce, cce})));
    }

    /**
     * Ensure exception is thrown if no cluster is found.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testEmptyList() throws GenieException {
        this.clb.selectCluster(new ArrayList<Cluster>());
    }

    /**
     * Ensure exception is thrown if no cluster is found.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testNullList() throws GenieException {
        this.clb.selectCluster(null);
    }
}
