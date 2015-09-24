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
package com.netflix.genie.core.jobmanager;

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.exceptions.GenieException;

/**
 * The interface to be implemented by job manager implementations.
 *
 * @author amsharma
 * @author tgianos
 * @since 2.0.0
 */
public interface JobManager {

    /**
     * Initialize the JobManager.
     *
     * @param job     The job this manager will be managing.
     * @param cluster The cluster this job will run on.
     * @throws GenieException On issue
     */
    void init(final Job job, final Cluster cluster) throws GenieException;

    /**
     * Launch the job.
     *
     * @throws GenieException On issue
     */
    void launch() throws GenieException;

    /**
     * Kill a job using the job information - no need to initialize this job.
     *
     * @throws GenieException On issue
     */
    void kill() throws GenieException;
}
