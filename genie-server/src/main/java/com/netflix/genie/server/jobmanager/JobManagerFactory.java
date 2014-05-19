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

import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.server.jobmanager.impl.HadoopJobManager;
import com.netflix.genie.server.jobmanager.impl.HiveJobManager;
import com.netflix.genie.server.jobmanager.impl.PigJobManager;
import com.netflix.genie.server.jobmanager.impl.YarnJobManager;

/**
 * Factory class to instantiate individual Hadoop/Hive/Pig job managers.
 *
 * @author skrishnan
 */
public final class JobManagerFactory {
    private static Logger logger = LoggerFactory
            .getLogger(JobManagerFactory.class);

    /**
     * Private constructor - never called for factory.
     */
    private JobManagerFactory() {
    }

    /**
     * Returns the right job manager for the job type.
     *
     * @param type
     *            the string describing the job - for eg: YARN, PRESTO
     * @return instance of the appropriate job manager
     * @throws Exception
     */
    public static JobManager getJobManager(String type)
            throws CloudServiceException {
        logger.info("called");

        // More Job Managers can be implemented here including
        // a generic one
        if (Types.JobType.YARN.name().equalsIgnoreCase(type)) {
            return new YarnJobManager();
        }

        String msg = String.format("JobManager [%s] is not supported", type);
        logger.error(msg);
        throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
    }
}
