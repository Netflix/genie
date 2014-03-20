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
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.ClusterConfigElementOld;
import com.netflix.genie.server.services.ClusterLoadBalancer;

/**
 * Basic implementation of a load balancer where a cluster is picked at random.
 *
 * @author skrishnan
 *
 */
public class RandomizedClusterLoadBalancerImpl implements ClusterLoadBalancer {

    private static Logger logger = LoggerFactory
            .getLogger(RandomizedClusterLoadBalancerImpl.class);

    /** {@inheritDoc} */
    @Override
    public ClusterConfigElementOld selectCluster(ClusterConfigElementOld[] ceArray)
            throws CloudServiceException {
        logger.info("called");

        if (ceArray == null || ceArray.length == 0) {
            String msg = "No cluster configuration found to match user params";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_PAYMENT_REQUIRED, msg);
        }

        // return a random one
        Random rand = new Random();
        return ceArray[Math.abs(rand.nextInt(ceArray.length))];
    }
}
