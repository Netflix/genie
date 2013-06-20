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

package com.netflix.genie.server.jobmanager.impl;

import java.io.File;
import java.net.HttpURLConnection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.HiveConfigResponse;
import com.netflix.genie.common.model.HiveConfigElement;
import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.services.HiveConfigService;
import com.netflix.genie.server.util.StringUtil;

/**
 * Implementation of job manager for Hive jobs.
 *
 * @author skrishnan
 */
public class HiveJobManager extends HadoopJobManager {

    private static Logger logger = LoggerFactory
            .getLogger(HiveJobManager.class);

    private HiveConfigService hcs;

    /**
     * Default constructor - initializes hive config impl.
     *
     * @throws CloudServiceException
     *             if there is an error in initialization
     */
    public HiveJobManager() throws CloudServiceException {
        super();
        hcs = ConfigServiceFactory.getHiveConfigImpl();
    }

    /**
     * Update command-line arguments specific to Hive/Genie.
     *
     * @return -hiveconf style params including genie job id and netflix
     *         environment
     */
    @Override
    protected String[] getGenieCmdArgs() {
        return new String[] {"-hiveconf", genieJobIDProp, "-hiveconf", netflixEnvProp};
    }

    /**
     * Initialize environment variables for Hive job.<br>
     * Be sure to call super.initEnv(ji) to initialize any common parameters.
     *
     * @param ji
     *            job info object for this job
     * @return a map containing environment variables for this job
     * @throws CloudServiceException
     */
    @Override
    protected Map<String, String> initEnv(JobInfoElement ji)
            throws CloudServiceException {
        logger.info("called");

        // initialize common hadoop environment
        Map<String, String> hEnv = super.initEnv(ji);

        // append hive-specific env here

        // get the hiveConfig - user param gets higher precedence
        String hiveConfigId = ji.getHiveConfigId();
        if (hiveConfigId == null) {
            hiveConfigId = cluster.getHiveConfigId(Types.Configuration.parse(ji
                    .getConfiguration()));
        }

        if (hiveConfigId == null) {
            String msg = "Cluster has no hiveConfigs for configuration: "
                    + ji.getConfiguration();
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        HiveConfigResponse hcr = hcs.getHiveConfig(hiveConfigId);
        HiveConfigElement[] hcArray = hcr.getHiveConfigs();

        if (hcArray == null || hcArray.length == 0) {
            String msg = "No hive configuration found to match hiveConfigID: "
                    + hiveConfigId;
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // ensure that hive config is not inactive
        String status = hcArray[0].getStatus();
        if (Types.ConfigStatus.parse(status) == Types.ConfigStatus.INACTIVE) {
            String msg = "Hive config " + hiveConfigId + " is INACTIVE";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        String s3HiveConfLocation = hcArray[0].getS3HiveSiteXml();
        hEnv.put("S3_HIVE_CONF_FILES", s3HiveConfLocation);

        // if the hive version is available, overwrite the HIVE_HOME environment
        // variable
        // user param gets higher precedence over cluster default
        String hiveVersion = ji.getHiveVersion();
        if ((hiveVersion == null) || hiveVersion.isEmpty()) {
            hiveVersion = hcArray[0].getHiveVersion();
        }

        // if the hive version is provided, overwrite the HIVE_HOME environment
        // variable
        if (hiveVersion != null) {
            // try exact version first
            String hiveHome = ConfigurationManager.getConfigInstance()
                    .getString(
                            "netflix.genie.server.hive." + hiveVersion
                                    + ".home");
            // if not, trim to 3 most significant digits
            if (hiveHome == null) {
                hiveVersion = StringUtil.trimVersion(hiveVersion);
                hiveHome = ConfigurationManager.getConfigInstance().getString(
                        "netflix.genie.server.hive." + hiveVersion + ".home");
            }

            if ((hiveHome == null) || (!new File(hiveHome).exists())) {
                String msg = "This genie instance doesn't support Hive version: "
                        + hiveVersion;
                logger.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }
            logger.info("Overriding HIVE_HOME from cluster config to: "
                    + hiveHome);
            hEnv.put("HIVE_HOME", hiveHome);
        }

        return hEnv;
    }
}
