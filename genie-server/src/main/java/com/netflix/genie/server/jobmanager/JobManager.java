/*
 *
 *  Copyright 2013 Netflix, Inc.
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

package com.netflix.genie.server.jobmanager;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.JobInfoElement;

/**
 * The interface to be implemented by Hadoop, Hive and Pig job manager
 * implementations.
 *
 * @author skrishnan
 */
public interface JobManager {

    /**
     * Initialize, and launch the job once it has been initialized.
     *
     * @param ji
     *            the JobInfo object for the job to be launched
     * @throws CloudServiceException
     */
    void launch(JobInfoElement ji) throws CloudServiceException;

    /**
     * Kill a job using the job information - no need to initialize this job.
     *
     * @param ji
     *            the JobInfo object for the job to be killed
     * @throws CloudServiceException
     */
    void kill(JobInfoElement ji) throws CloudServiceException;
}
