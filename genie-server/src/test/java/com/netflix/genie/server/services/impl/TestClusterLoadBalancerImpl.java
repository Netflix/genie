/*
 *
 *  Copyright 2013 Netflix, Inc.
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

import java.net.HttpURLConnection;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.ClusterConfigElement;
import com.netflix.genie.server.services.ClusterLoadBalancer;

/**
 * Test for the cluster load balancer.
 *
 * @author skrishnan
 */
public class TestClusterLoadBalancerImpl {

    /**
     * Test whether a cluster is returned from a set of candidates.
     *
     * @throws Exception if anything went wrong with the test.
     */
    @Test
    public void testValidCluster() throws Exception {
        ClusterConfigElement cce = new ClusterConfigElement();
        ClusterLoadBalancer clb = new RandomizedClusterLoadBalancerImpl();
        Assert.assertNotNull(clb.selectCluster(new ClusterConfigElement[] {
                cce, cce, cce }));
    }

    /**
     * Test whether HttpURLConnection.HTTP_PAYMENT_REQUIRED is raised if a cluster
     * can't be found from chttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/commandhttp://localhost:7001/genie/v1/config/command((applicationId != null) && (applicationId.isEmpty()))((applicationId != null) && (applicationId.isEmpty()))((applicationId != null) && (applicationId.isEmpty()))andidates.
     */
    @Test
    public void testInvalidCluster() {
        ClusterLoadBalancer clb = new RandomizedClusterLoadBalancerImpl();
        try {
            clb.selectCluster(new ClusterConfigElement[] {});
        } catch (CloudServiceException cse) {
            Assert.assertEquals(cse.getErrorCode(),
                    HttpURLConnection.HTTP_PAYMENT_REQUIRED);
        }
        try {
            clb.selectCluster(null);
        } catch (CloudServiceException cse) {
            Assert.assertEquals(cse.getErrorCode(),
                    HttpURLConnection.HTTP_PAYMENT_REQUIRED);
        }
    }
}
