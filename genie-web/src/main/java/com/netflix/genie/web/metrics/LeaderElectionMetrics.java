/*
 *
 *  Copyright 2026 Netflix, Inc.
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
package com.netflix.genie.web.metrics;

import com.netflix.genie.web.services.ClusterLeaderService;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Metrics describing this node's application-level Genie leader election state.
 */
public class LeaderElectionMetrics {

    /** Gauge indicating whether this node is the elected Genie leader. */
    public static final String LEADER_GAUGE_NAME = "genie.leaderElection.leader.gauge";

    // Micrometer gauges hold their state object weakly, so keep a strong reference to prevent it from being GCed
    // https://docs.micrometer.io/micrometer/reference/concepts/gauges.html#_why_is_my_gauge_reporting_nan_or_disappearing
    private final ClusterLeaderService clusterLeaderService;

    /**
     * Constructor.
     *
     * @param clusterLeaderService the service exposing current leader-election state
     * @param registry             the metrics registry
     */
    public LeaderElectionMetrics(
        final ClusterLeaderService clusterLeaderService,
        final MeterRegistry registry
    ) {
        this.clusterLeaderService = clusterLeaderService;
        registry.gauge(
            LEADER_GAUGE_NAME,
            this.clusterLeaderService,
            service -> service.isLeader() ? 1.0 : 0.0
        );
    }
}
