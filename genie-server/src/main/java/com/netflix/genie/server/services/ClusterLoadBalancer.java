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

package com.netflix.genie.server.services;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.ClusterConfig;

/**
 * Interface for the cluster load-balancer, which returns the "best" cluster to
 * run job on from an array of candidates.
 *
 * @author skrishnan
 *
 */
public interface ClusterLoadBalancer {

    /**
     * Return best cluster to run job on.
     *
     * @param ceArray
     *            array of cluster to choose from
     * @return the "best" cluster to run job on
     * @throws CloudServiceException
     *             if there is any error
     */
    ClusterConfig selectCluster(ClusterConfig[] ceArray)
            throws CloudServiceException;
}
