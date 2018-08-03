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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobKillServiceV4;
import com.netflix.genie.web.services.JobPersistenceService;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;

/**
 * Implementation of the JobKillService interface.
 * Attempts to kill v3 jobs running on the local genie nodes.
 * Attempts to kill v4 jobs running on remote agents.
 *
 * @author standon
 * @since 4.0.0
 */
@Slf4j
public class JobKillServiceImpl implements JobKillService {

    private final JobKillServiceV3 jobKillServiceV3;
    private final JobKillServiceV4 jobKillServiceV4;
    private final JobPersistenceService jobPersistenceService;

    /**
     * Constructor.
     *
     * @param jobKillServiceV3      Service to kill V3 jobs.
     * @param jobKillServiceV4      Service to kill V4 jobs.
     * @param jobPersistenceService Job persistence service
     */
    public JobKillServiceImpl(
        final JobKillServiceV3 jobKillServiceV3,
        final JobKillServiceV4 jobKillServiceV4,
        final JobPersistenceService jobPersistenceService

    ) {
        this.jobKillServiceV3 = jobKillServiceV3;
        this.jobKillServiceV4 = jobKillServiceV4;
        this.jobPersistenceService = jobPersistenceService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void killJob(
        @NotBlank(message = "No id entered. Unable to kill job.") final String id,
        @NotBlank(message = "No reason provided.") final String reason
    ) throws GenieException {

        if (jobPersistenceService.isV4(id)) {
            jobKillServiceV4.killJob(id, reason);
        } else {
            jobKillServiceV3.killJob(id, reason);
        }
    }
}
