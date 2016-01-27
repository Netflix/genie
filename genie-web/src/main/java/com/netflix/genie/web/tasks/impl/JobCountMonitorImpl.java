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
package com.netflix.genie.web.tasks.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.metrics.GenieNodeStatistics;
import com.netflix.genie.core.metrics.JobCountManager;
import com.netflix.genie.web.tasks.JobCountMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Monitor thread that routinely updates the statistics object.
 *
 * @author tgianos
 * @since 1.0.0
 */
//TODO: Add conditionals to enable or disable
@Component
public class JobCountMonitorImpl implements JobCountMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(JobCountMonitorImpl.class);

    private final JobCountManager jobCountManager;
    private final GenieNodeStatistics stats;

    @Value("${com.netflix.genie.server.metrics.sleep.ms:30000}")
    private long metricsSleepTime;

    /**
     * Constructor.
     *
     * @param stats reference to the statistics object that must be updated
     * @param jobCountManager The job count manager
     */
    @Autowired
    public JobCountMonitorImpl(final GenieNodeStatistics stats, final JobCountManager jobCountManager) {
        this.jobCountManager = jobCountManager;
        this.stats = stats;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Scheduled(fixedRate = 60000)  // TODO: Randomize? Do we need this to be a property?
    public void updateJobCounts() throws GenieException {
        LOG.info("Updating job counts...");

        final long time = System.currentTimeMillis();

        this.stats.setGenieRunningJobs(
                this.jobCountManager.getNumInstanceJobs()
        );
        this.stats.setGenieRunningJobs0To15m(
                this.jobCountManager.getNumInstanceJobs(time - 15 * 60 * 1000, null)
        );
        this.stats.setGenieRunningJobs15mTo2h(
                this.jobCountManager.getNumInstanceJobs(time - 2 * 60 * 60 * 1000, time - 15 * 60 * 1000)
        );
        this.stats.setGenieRunningJobs2hTo8h(
                this.jobCountManager.getNumInstanceJobs(time - 8 * 60 * 60 * 1000, time - 2 * 60 * 60 * 1000)
        );
        this.stats.setGenieRunningJobs8hPlus(
                this.jobCountManager.getNumInstanceJobs(null, time - 8 * 60 * 60 * 1000)
        );

        LOG.info("Finished updating job counts");
    }
}
