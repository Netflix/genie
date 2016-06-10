/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.health;

import com.netflix.genie.web.tasks.job.JobMonitoringCoordinator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;

/**
 * A health indicator based around metrics from the Genie system most notably the number of running jobs relative to
 * the max configured.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Component
public class GenieHealthIndicator implements HealthIndicator {

    private static final String MAX_RUNNING_JOBS_KEY = "maxRunningJobs";
    private static final String NUMBER_RUNNING_JOBS_KEY = "numRunningJobs";

    private final JobMonitoringCoordinator jobMonitoringCoordinator;
    private final int maxRunningJobs;

    /**
     * Constructor.
     *
     * @param jobMonitoringCoordinator The job monitoring coordinator used to check how many jobs are running
     * @param maxRunningJobs           The maximum number of jobs that can run on this node
     */
    @Autowired
    public GenieHealthIndicator(
        @NotNull final JobMonitoringCoordinator jobMonitoringCoordinator,
        @Value("${genie.jobs.max.running:2}") final int maxRunningJobs
    ) {
        this.jobMonitoringCoordinator = jobMonitoringCoordinator;
        this.maxRunningJobs = maxRunningJobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Health health() {
        final int numRunningJobs = this.jobMonitoringCoordinator.getNumRunningJobs();
        if (numRunningJobs < this.maxRunningJobs) {
            return Health
                .up()
                .withDetail(MAX_RUNNING_JOBS_KEY, this.maxRunningJobs)
                .withDetail(NUMBER_RUNNING_JOBS_KEY, numRunningJobs)
                .build();
        } else {
            return Health
                .outOfService()
                .withDetail(MAX_RUNNING_JOBS_KEY, this.maxRunningJobs)
                .withDetail(NUMBER_RUNNING_JOBS_KEY, numRunningJobs)
                .build();
        }
    }
}
