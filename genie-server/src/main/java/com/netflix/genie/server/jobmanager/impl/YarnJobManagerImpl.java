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
package com.netflix.genie.server.jobmanager.impl;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.server.jobmanager.JobMonitor;
import com.netflix.genie.server.services.CommandConfigService;
import com.netflix.genie.server.services.JobService;
import com.netflix.genie.server.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Implementation of job manager for Yarn Jobs.
 *
 * @author amsharma
 * @author tgianos
 */
public class YarnJobManagerImpl extends JobManagerImpl {

    private static final Logger LOG = LoggerFactory.getLogger(YarnJobManagerImpl.class);
    private static final String COPY_COMMAND_KEY = "com.netflix.genie.server.job.manager.yarn.command.cp";
    private static final String MAKE_DIRECTORY_COMMAND_KEY = "com.netflix.genie.server.job.manager.yarn.command.mkdir";

    //TODO: Move to a property file
    /**
     * The name of the Genie job id property to be passed to all jobs.
     */
    private static final String GENIE_JOB_ID = "genie.job.id";

    //TODO: Move to a property file
    /**
     * The name of the Environment (test/prod) property to be passed to all
     * jobs.
     */
    private static final String EXECUTION_ENVIRONMENT = "netflix.environment";

    /**
     * Default constructor - initializes cluster configuration and load
     * balancer.
     *
     * @param jobMonitor     The job monitor object to use.
     * @param jobService     The job service to use.
     * @param commandService The command service to use.
     */
    public YarnJobManagerImpl(final JobMonitor jobMonitor,
                              final JobService jobService,
                              final CommandConfigService commandService) {
        super(jobMonitor, jobService, commandService);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void launch() throws GenieException {
        LOG.info("called");
        if (!this.isInitCalled()) {
            throw new GeniePreconditionException("Init wasn't called. Unable to continue.");
        }

        // create the ProcessBuilder for this process
        final List<String> processArgs = this.createBaseProcessArguments();
        processArgs.addAll(Arrays.asList(StringUtil.splitCmdLine(this.getJob().getCommandArgs())));

        final ProcessBuilder processBuilder = new ProcessBuilder(processArgs);

        // construct the environment variables
        this.setupCommonProcess(processBuilder);
        this.setupYarnProcess(processBuilder);

        // Launch the actual process
        this.launchProcess(
                processBuilder,
                ConfigurationManager
                        .getConfigInstance()
                        .getInt("com.netflix.genie.server.job.manager.yarn.sleeptime", 5000)
        );
    }

    /**
     * Set up the process with specific YARN properties.
     *
     * @param processBuilder The process builder to modify.
     * @throws GenieException If there are any issues.
     */
    private void setupYarnProcess(final ProcessBuilder processBuilder) throws GenieException {
        // setup the HADOOP specific variables
        final Map<String, String> processEnv = processBuilder.environment();
        processEnv.put("HADOOP_CONF_DIR", this.getJobDir() + "/conf");
        processEnv.put("HADOOP_USER_NAME", this.getJob().getUser());
        processEnv.put("HADOOP_GROUP_NAME", this.getGroupName());

        // set the variables to be added to the core-site xml. Format of this variable is:
        // key1=value1;key2=value2;key3=value3
        final String genieJobIDProp = GENIE_JOB_ID + "=" + this.getJob().getId();
        final String netflixEnvProp = EXECUTION_ENVIRONMENT
                + "="
                + ConfigurationManager.getConfigInstance().getString("netflix.environment");

        final String lipstickUuidPropName = ConfigurationManager.getConfigInstance()
                .getString("com.netflix.genie.server.lipstick.uuid.prop.name", "lipstick.uuid.prop.name");

        final String lipstickUuidProp;
        if (ConfigurationManager.getConfigInstance().getBoolean("com.netflix.genie.server.lipstick.enable", false)) {
            lipstickUuidProp = lipstickUuidPropName + "=" + GENIE_JOB_ID;
        } else {
            lipstickUuidProp = "";
        }
        processEnv.put(
                "CORE_SITE_XML_ARGS",
                StringUtils.join(
                        new String[]{
                                genieJobIDProp,
                                netflixEnvProp,
                                lipstickUuidProp
                        },
                        JobManagerImpl.SEMI_COLON)
        );

        // if the cluster version is provided, overwrite the HADOOP_HOME
        // environment variable
        String hadoopHome;
        if (this.getCluster().getVersion() != null) {
            String hadoopVersion = this.getCluster().getVersion();
            LOG.debug("Hadoop Version of the cluster: " + hadoopVersion);

            // try extract version first
            hadoopHome = ConfigurationManager
                    .getConfigInstance()
                    .getString("com.netflix.genie.server.hadoop." + hadoopVersion + ".home");
            // if not, trim to 3 most significant digits
            if (hadoopHome == null) {
                hadoopVersion = StringUtil.trimVersion(hadoopVersion);
                hadoopHome = ConfigurationManager.getConfigInstance()
                        .getString("com.netflix.genie.server.hadoop." + hadoopVersion + ".home");
            }

            if (hadoopHome == null || !new File(hadoopHome).exists()) {
                String msg = "This genie instance doesn't support Hadoop version: "
                        + hadoopVersion;
                LOG.error(msg);
                throw new GenieServerException(msg);
            }

            LOG.info("Overriding HADOOP_HOME from cluster config to: "
                    + hadoopHome);
            processEnv.put("HADOOP_HOME", hadoopHome);
        } else {
            // set the default hadoop home
            hadoopHome = ConfigurationManager
                    .getConfigInstance()
                    .getString("com.netflix.genie.server.hadoop.home");
            if (hadoopHome == null || !new File(hadoopHome).exists()) {
                final String msg = "Property com.netflix.genie.server.hadoop.home is not set correctly";
                LOG.error(msg);
                throw new GenieServerException(msg);
            }
            processEnv.put("HADOOP_HOME", hadoopHome);
        }

        processEnv.put("CP_TIMEOUT",
                ConfigurationManager.getConfigInstance()
                        .getString("com.netflix.genie.server.hadoop.s3cp.timeout", "1800"));

        final String copyCommand =
                ConfigurationManager.getConfigInstance()
                        .getString(COPY_COMMAND_KEY);
        if (StringUtils.isBlank(copyCommand)) {
            final String msg = "Required property " + COPY_COMMAND_KEY + " isn't set";
            LOG.error(msg);
            throw new GenieServerException(msg);
        }
        processEnv.put("COPY_COMMAND", copyCommand);

        // Force flag to overwrite required in Hadoop2
        processEnv.put("FORCE_COPY_FLAG", "-f");

        final String makeDirCommand =
                ConfigurationManager.getConfigInstance()
                        .getString(MAKE_DIRECTORY_COMMAND_KEY);
        if (StringUtils.isBlank(makeDirCommand)) {
            final String msg = "Required property " + MAKE_DIRECTORY_COMMAND_KEY + " isn't set";
            LOG.error(msg);
            throw new GenieServerException(msg);
        }
        processEnv.put("MKDIR_COMMAND", makeDirCommand);
    }
}
