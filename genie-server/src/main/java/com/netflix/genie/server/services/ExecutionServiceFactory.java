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

package com.netflix.genie.server.services;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Factory class to instantiate the ExecutionService.
 *
 * @author skrishnan
 */
public final class ExecutionServiceFactory extends BaseServiceFactory {

    private static Logger logger = LoggerFactory
            .getLogger(ExecutionServiceFactory.class);

    // instance of the netflix configuration object
    private static AbstractConfiguration conf = ConfigurationManager
            .getConfigInstance();

    // environment variables needed by the job, initialized from
    // application.properties
    private static Map<String, String> jobEnv = new HashMap<String, String>();

    // instances of the possible implementations
    private static volatile ExecutionService execService;

    // initialize static variables
    static {
        jobEnv.put("JAVA_HOME",
                conf.getString("netflix.genie.server.java.home"));
        // these are defaults - can be overridden at the job level
        jobEnv.put("HADOOP_HOME",
                conf.getString("netflix.genie.server.hadoop.home"));
        jobEnv.put("HIVE_HOME",
                conf.getString("netflix.genie.server.hive.home"));
        jobEnv.put("PIG_HOME", conf.getString("netflix.genie.server.pig.home"));

        // genie system home
        jobEnv.put("XS_SYSTEM_HOME",
                conf.getString("netflix.genie.server.sys.home"));

        // root directory for USER jobs
        jobEnv.put("BASE_USER_WORKING_DIR",
                conf.getString("netflix.genie.server.user.working.dir"));

        // base directory for archiving logs
        String s3ArchiveLocation =
                conf.getString("netflix.genie.server.s3.archive.location");
        if (s3ArchiveLocation != null) {
            jobEnv.put("S3_ARCHIVE_LOCATION", s3ArchiveLocation);
        }
    }

    /**
     * Get job environment for this instance.
     *
     * @return a map of environment variables and corresponding values
     */
    public static Map<String, String> getJobEnv() {
        return jobEnv;
    }

    /**
     * Get an instance of the configured execution service impl.
     *
     * @return singleton execution service impl
     * @throws CloudServiceException
     */
    public static synchronized ExecutionService getExecutionServiceImpl()
            throws CloudServiceException {
        if (execService == null) {
            logger.info("Instantiating execution service impl");
            execService = (ExecutionService) instantiateFromProperty("netflix.genie.server.executionServiceImpl");
        }
        return execService;
    }
}
