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

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.web.services.ClusterLoadBalancer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotEmpty;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

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
    public Cluster selectCluster(
        @Nonnull @NonNull @NotEmpty final Set<Cluster> clusters,
        @Nonnull @NonNull final JobRequest jobRequest
    ) throws GenieException {
        log.debug("called");

        // return a random one
        final Random rand = new Random();

        return new ArrayList<>(clusters).get(Math.abs(rand.nextInt(clusters.size())));
    }
}
