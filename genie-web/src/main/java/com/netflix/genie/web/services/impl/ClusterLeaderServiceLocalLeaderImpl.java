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
import com.netflix.genie.web.tasks.leader.LocalLeader;

/**
 * Implementation of {@link ClusterLeaderService} using statically configured {@link LocalLeader} module.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class ClusterLeaderServiceLocalLeaderImpl implements ClusterLeaderService {
    private final LocalLeader localLeader;

    /**
     * Constructor.
     *
     * @param localLeader the local leader module
     */
    public ClusterLeaderServiceLocalLeaderImpl(final LocalLeader localLeader) {
        this.localLeader = localLeader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        this.localLeader.stop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        this.localLeader.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isRunning() {
        return this.localLeader.isRunning();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeader() {
        return this.localLeader.isLeader();
    }
}
