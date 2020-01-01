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
package com.netflix.genie.web.services;

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.Command;

import java.util.List;

/**
 * A service which defines the three basic stages of a job.
 *
 * @author amajumdar
 * @since 3.0.0
 */
public interface JobStateService extends JobMetricsService {
    /**
     * Initialize the job.
     *
     * @param jobId job id
     */
    void init(String jobId);

    /**
     * Schedules the job.
     *
     * @param jobId        job id
     * @param jobRequest   job request
     * @param cluster      cluster for the job request based on the tags specified
     * @param command      command for the job request based on command tags and cluster chosen
     * @param applications applications to use based on the command that was selected
     * @param memory       job memory
     */
    void schedule(
        String jobId,
        JobRequest jobRequest,
        Cluster cluster,
        Command command,
        List<Application> applications,
        int memory
    );

    /**
     * Called when the job is done.
     *
     * @param jobId job id
     * @throws GenieException on unrecoverable error
     */
    void done(String jobId) throws GenieException;

    /**
     * Returns true if the job exists locally.
     *
     * @param jobId job id
     * @return true if job exists
     */
    boolean jobExists(String jobId);
}
