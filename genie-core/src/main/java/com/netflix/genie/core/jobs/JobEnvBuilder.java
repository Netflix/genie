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
package com.netflix.genie.core.jobs;

import com.netflix.genie.common.dto.JobRequest;

/**
 * Contains the logic to get all the details needed to run a job.
 * Resolves the criteria provided in the job request to construct the
 * JobExecutionEnvironment dto.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class JobEnvBuilder {

    /**
     * Method that accepts a jobRequest object and uses the criteria selected to build a
     * jobExecutionEnvironmentBuilder object.
     *
     * @param jr The job request object
     * @return jobExecutionEnvironment
     */
    public JobExecutionEnvironment build(final JobRequest jr) {
        return null;
    }
}
