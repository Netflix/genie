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
package com.netflix.genie.web.services.impl;

import com.netflix.genie.web.services.JobMetricsService;
import com.netflix.genie.web.services.JobSearchService;

import javax.validation.constraints.NotNull;

/**
 * A default implementation of the job count service which uses the job search service to find the running jobs on
 * the host.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JobMetricsServiceImpl implements JobMetricsService {

    private final JobSearchService jobSearchService;
    private final String hostname;

    /**
     * Constructor.
     *
     * @param jobSearchService The job search service to use.
     * @param hostname         The name of this host
     */
    public JobMetricsServiceImpl(@NotNull final JobSearchService jobSearchService, @NotNull final String hostname) {
        this.jobSearchService = jobSearchService;
        this.hostname = hostname;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumActiveJobs() {
        return this.jobSearchService.getAllActiveJobsOnHost(this.hostname).size();
    }

    /**
     * Get the amount of memory currently used by jobs in MB.
     *
     * @return The total memory used by jobs in megabytes
     */
    @Override
    public int getUsedMemory() {
        //TODO: Fill this out with query from database
        return 0;
    }
}
