/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Job;

/**
 * The interface to be implemented by job manager implementations.
 *
 * @author skrishnan
 * @author amsharma
 */
public interface JobManager {

    /**
     * Initialize, and launch the job once it has been initialized.
     *
     * @param job the JobInfo object for the job to be launched
     * @throws com.netflix.genie.common.exceptions.GenieException
     */
    void launch(final Job job) throws GenieException;

    /**
     * Kill a job using the job information - no need to initialize this job.
     *
     * @param job the JobInfo object for the job to be killed
     * @throws com.netflix.genie.common.exceptions.GenieException
     */
    void kill(final Job job) throws GenieException;

    /**
     * Set the cluster to use for the job.
     *
     * @param cluster The cluster to set. Not null.
     * @throws com.netflix.genie.common.exceptions.GenieException
     */
    void setCluster(final Cluster cluster) throws GenieException;
}
