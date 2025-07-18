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
package com.netflix.genie.web.services.impl;

import com.netflix.genie.web.services.ClusterLeaderService;
import org.springframework.integration.leader.Context;
import org.springframework.integration.zookeeper.leader.LeaderInitiator;

/**
 * Implementation of {@link ClusterLeaderService} using Spring's {@link LeaderInitiator} (Zookeeper/Curator based
 * leader election mechanism).
 *
 * @author mprimi
 * @since 4.0.0
 */
public class ClusterLeaderServiceCuratorImpl implements ClusterLeaderService {

    private LeaderInitiator leaderInitiator;

    /**
     * Constructor.
     *
     * @param leaderInitiator the leader initiator component
     */
    public ClusterLeaderServiceCuratorImpl(final LeaderInitiator leaderInitiator) {
        this.leaderInitiator = leaderInitiator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        this.leaderInitiator.stop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        this.leaderInitiator.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isRunning() {
        return this.leaderInitiator.isRunning();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeader() {
        final Context context = this.leaderInitiator.getContext();
        return context != null && context.isLeader();
    }
}
