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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LeaderElectionMetrics}.
 */
class LeaderElectionMetricsTest {

    private SimpleMeterRegistry registry;
    private ClusterLeaderService clusterLeaderService;
    /**
     * Set up a metrics registry and a leader service for each test.
     */
    @BeforeEach
    void setUp() {
        this.registry = new SimpleMeterRegistry();
        this.clusterLeaderService = Mockito.mock(ClusterLeaderService.class);
        new LeaderElectionMetrics(this.clusterLeaderService, this.registry);
    }

    /**
     * The leader gauge reflects the leadership state at observation time (rather than just the last event).
     */
    @Test
    void leaderGaugeTracksCurrentServiceState() {
        Mockito.when(this.clusterLeaderService.isLeader()).thenReturn(false, true, false);

        Assertions.assertThat(this.registry.get(LeaderElectionMetrics.LEADER_GAUGE_NAME).gauge().value())
            .isEqualTo(0.0);
        Assertions.assertThat(this.registry.get(LeaderElectionMetrics.LEADER_GAUGE_NAME).gauge().value())
            .isEqualTo(1.0);
        Assertions.assertThat(this.registry.get(LeaderElectionMetrics.LEADER_GAUGE_NAME).gauge().value())
            .isEqualTo(0.0);
    }
}
