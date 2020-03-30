/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.services;

/**
 * Service interface for the abstracts the details of leadership within nodes in a Genie cluster.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface ClusterLeaderService {

    /**
     * Stop the service (i.e. renounce leadership and leave the election).
     */
    void stop();

    /**
     * Start the service (i.e. join the the election).
     */
    void start();

    /**
     * Whether or not this node is participating in the cluster leader election.
     *
     * @return true if the node is participating in leader election
     */
    boolean isRunning();

    /**
     * Whether or not this node is the current cluster leader.
     *
     * @return true if the node is the current cluster leader
     */
    boolean isLeader();
}
