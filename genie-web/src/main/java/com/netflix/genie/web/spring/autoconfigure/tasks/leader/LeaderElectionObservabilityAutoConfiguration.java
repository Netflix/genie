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
package com.netflix.genie.web.spring.autoconfigure.tasks.leader;

import com.netflix.genie.web.metrics.LeaderElectionMetrics;
import com.netflix.genie.web.properties.LeaderSchedulerHeartbeatProperties;
import com.netflix.genie.web.services.ClusterLeaderService;
import com.netflix.genie.web.tasks.leader.LeaderSchedulerHeartbeatTask;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration for application-level Genie leader-election observability (metrics and heartbeat task).
 *
 * <p>This configuration runs after {@link LeaderAutoConfiguration} so that {@link ClusterLeaderService}
 * is available.</p>
 *
 */
@AutoConfiguration(after = LeaderAutoConfiguration.class)
@EnableConfigurationProperties(LeaderSchedulerHeartbeatProperties.class)
@ConditionalOnBean(ClusterLeaderService.class)
public class LeaderElectionObservabilityAutoConfiguration {

    /**
     * Register leadership metrics.
     *
     * @param clusterLeaderService the application leader-election service
     * @param registry             the metrics registry
     * @return the leader metrics
     */
    @Bean
    @ConditionalOnMissingBean(LeaderElectionMetrics.class)
    public LeaderElectionMetrics leaderElectionMetrics(
        final ClusterLeaderService clusterLeaderService,
        final MeterRegistry registry
    ) {
        return new LeaderElectionMetrics(clusterLeaderService, registry);
    }

    /**
     * Register the leader-only heartbeat task for the default {@code LeaderTasksCoordinator} to schedule.
     *
     * @param registry the metrics registry
     * @return the heartbeat task
     */
    @Bean
    @ConditionalOnProperty(
        value = LeaderSchedulerHeartbeatProperties.ENABLED_PROPERTY,
        havingValue = "true",
        matchIfMissing = true
    )
    @ConditionalOnMissingBean(LeaderSchedulerHeartbeatTask.class)
    public LeaderSchedulerHeartbeatTask leaderSchedulerHeartbeatTask(final MeterRegistry registry) {
        return new LeaderSchedulerHeartbeatTask(registry);
    }
}
