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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;

import java.util.List;
import java.util.Random;

/**
 * Basic implementation of a load balancer where a cluster is picked at random.
 *
 * @author skrishnan
 * @author tgianos
 */
@Slf4j
public class RandomizedClusterLoadBalancerImpl implements ClusterLoadBalancer {

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster selectCluster(final List<Cluster> clusters, final JobRequest jobRequest) throws GenieException {
        log.debug("called");

        if (clusters == null || clusters.isEmpty()) {
            final String msg = "No cluster configuration found for supplied cluster criteria";
            log.error(msg);
            throw new GeniePreconditionException(msg);
        }

        // return a random one
        final Random rand = new Random();
        return clusters.get(Math.abs(rand.nextInt(clusters.size())));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
