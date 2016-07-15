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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.core.services.JobCountService;
import com.netflix.genie.core.services.JobSearchService;

import javax.validation.constraints.NotNull;

/**
 * A default implementation of the job count service which uses the job search service to find the running jobs on
 * the host.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JobCountServiceImpl implements JobCountService {

    private final JobSearchService jobSearchService;
    private final String hostName;

    /**
     * Constructor.
     *
     * @param jobSearchService The job search service to use.
     * @param hostName         The name of this host
     */
    public JobCountServiceImpl(@NotNull final JobSearchService jobSearchService, @NotNull final String hostName) {
        this.jobSearchService = jobSearchService;
        this.hostName = hostName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumJobs() {
        return this.jobSearchService.getAllRunningJobExecutionsOnHost(this.hostName).size();
    }
}
