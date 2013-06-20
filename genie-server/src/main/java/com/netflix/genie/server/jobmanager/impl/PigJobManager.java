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
import com.netflix.genie.common.messages.PigConfigResponse;
import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.common.model.PigConfigElement;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.services.PigConfigService;
import com.netflix.genie.server.util.StringUtil;

/**
 * Implementation of job manager for Pig jobs.
 *
 * @author skrishnan
 */
public class PigJobManager extends HadoopJobManager {

    private static Logger logger = LoggerFactory.getLogger(PigJobManager.class);

    private PigConfigService pcs;

    /**
     * Default constructor - initializes pig config impl.
     *
     * @throws CloudServiceException
     *             if there is an exception during initialization
     */
    public PigJobManager() throws CloudServiceException {
        super();
        pcs = ConfigServiceFactory.getPigConfigImpl();
    }

    /**
     * Override/add additional environment variables for Pig job.
     *
     * @param ji
     *            job info object for this job
     * @return a map containing environment variables for this job
     * @throws CloudServiceException
     *             if there is an exception in the initialization
     */
    @Override
    protected Map<String, String> initEnv(JobInfoElement ji)
            throws CloudServiceException {
        logger.info("called");

        // initialize common hadoop environment
        Map<String, String> hEnv = super.initEnv(ji);

        // append pig-specific env here

        // get the pig config - user param gets higher precedence
        String pigConfigId = ji.getPigConfigId();
        if (pigConfigId == null) {
            pigConfigId = cluster.getPigConfigId(Types.Configuration.parse(ji
                    .getConfiguration()));
        }
        if (pigConfigId == null) {
            String msg = "Cluster has no pigConfigs for configuration: "
                    + ji.getConfiguration();
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        PigConfigResponse pcr = pcs.getPigConfig(pigConfigId);
        PigConfigElement[] pcArray = pcr.getPigConfigs();
        if (pcArray == null || pcArray.length == 0) {
            String msg = "No pig configuration found to match pigConfigID: "
                    + pigConfigId;
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // ensure that pig config is not inactive
        String status = pcArray[0].getStatus();
        if (Types.ConfigStatus.parse(status) == Types.ConfigStatus.INACTIVE) {
            String msg = "Pig config " + pigConfigId + " is INACTIVE";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        String s3PigProps = pcArray[0].getS3PigProperties();
        hEnv.put("S3_PIG_CONF_FILES", s3PigProps);

        // if the pig version is available, overwrite the PIG_HOME environment
        // variable
        // user param gets higher precedence over cluster default
        String pigVersion = ji.getPigVersion();
        if ((pigVersion == null) || pigVersion.isEmpty()) {
            pigVersion = pcArray[0].getPigVersion();
        }
        if (pigVersion != null) {
            // try exact version first
            String pigHome = ConfigurationManager.getConfigInstance()
                    .getString(
                            "netflix.genie.server.pig." + pigVersion + ".home");
            // if not, trim to 3 most significant digits
            if (pigHome == null) {
                pigVersion = StringUtil.trimVersion(pigVersion);
                pigHome = ConfigurationManager.getConfigInstance().getString(
                        "netflix.genie.server.pig." + pigVersion + ".home");
            }

            if ((pigHome == null) || (!new File(pigHome).exists())) {
                String msg = "This genie instance doesn't support Pig version: "
                        + pigVersion;
                logger.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }
            logger.info("Overriding PIG_HOME from cluster config to: "
                    + pigHome);
            hEnv.put("PIG_HOME", pigHome);
        }

        // add the pigOverrideUrl, if provided
        String pigOverrideUrl = ji.getPigOverrideUrl();
        if ((pigOverrideUrl != null) && (!pigOverrideUrl.isEmpty())) {
            hEnv.put("PIG_OVERRIDE_URL", pigOverrideUrl);
        }

        return hEnv;
    }
}
