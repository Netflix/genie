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
package com.netflix.genie.web.tasks.leader;

import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * A leader-only task that records heartbeats from the shared leader-task scheduler.
 *
 * <p>When enabled via the `genie.tasks.leader-task-heartbeat.enabled` property, this task is scheduled to run
 * every 10 seconds and increments a counter when it runs. Consumers should evaluate its increment rate only for a node
 * that is currently the leader (see {@code LeaderElectionMetrics}).</p>
 *
 */
public class LeaderSchedulerHeartbeatTask extends LeaderTask {

    /** Heartbeat metric name. */
    public static final String HEARTBEAT_COUNTER_NAME = "genie.leaderElection.heartbeat.counter";

    /** Interval between heartbeat executions in ms. */
    public static final long HEARTBEAT_INTERVAL_MILLIS = 10_000L;

    private final Counter heartbeatCounter;

    /**
     * Constructor.
     *
     * @param registry the metrics registry
     */
    public LeaderSchedulerHeartbeatTask(final MeterRegistry registry) {
        this.heartbeatCounter = registry.counter(HEARTBEAT_COUNTER_NAME);
    }

    /**
     * Increment the heartbeat once per task execution.
     */
    @Override
    public void run() {
        this.heartbeatCounter.increment();
    }

    /**
     * Do nothing on cleanup.
     */
    @Override
    public void cleanup() {
        // Intentionally no-op; keep the value of the counter across leadership changes
    }

    /**
     * {@inheritDoc}
     *
     * @return a fixed-rate schedule
     */
    @Override
    public GenieTaskScheduleType getScheduleType() {
        return GenieTaskScheduleType.FIXED_RATE;
    }

    /**
     * {@inheritDoc}
     *
     * @return heartbeat interval
     */
    @Override
    public long getFixedRate() {
        return HEARTBEAT_INTERVAL_MILLIS;
    }
}
