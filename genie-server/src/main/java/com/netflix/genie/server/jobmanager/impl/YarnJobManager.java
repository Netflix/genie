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
package com.netflix.genie.server.jobmanager.impl;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.server.jobmanager.JobMonitor;
import com.netflix.genie.server.services.JobService;
import com.netflix.genie.server.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.util.Map;

/**
 * Implementation of job manager for Yarn Jobs.
 *
 * @author amsharma
 * @author skrishnan
 * @author bmundlapudi
 * @author tgianos
 */
@Named
@Scope("prototype")
public class YarnJobManager extends JobManagerImpl {

    private static final Logger LOG = LoggerFactory.getLogger(YarnJobManager.class);

    /**
     * Default constructor - initializes cluster configuration and load
     * balancer.
     *
     * @param jobMonitor The job monitor object to use.
     * @param jobService The job service to use.
     */
    @Inject
    public YarnJobManager(final JobMonitor jobMonitor,
                          final JobService jobService) {
        super(jobMonitor, jobService);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final Job job, final Cluster cluster) throws GenieException {
        LOG.info("called");
        super.init(job, cluster);

        // setup the HADOOP specific variables
        final Map<String, String> processEnv = this.getProcessBuilder().environment();
        processEnv.put("HADOOP_CONF_DIR", this.getJobDir() + "/conf");
        processEnv.put("HADOOP_USER_NAME", this.getJob().getUser());
        processEnv.put("HADOOP_GROUP_NAME", this.getGroupName());

        // set the variables to be added to the core-site xml. Format of this variable is:
        // key1=value1;key2=value2;key3=value3
        final String genieJobIDProp = GENIE_JOB_ID + "=" + this.getJob().getId();
        final String netflixEnvProp = NETFLIX_ENV
                + "="
                + ConfigurationManager.getConfigInstance().getString("netflix.environment");

        final String lipstickUuidPropName = ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.lipstick.uuid.prop.name", "lipstick.uuid.prop.name");

        final String lipstickUuidProp;
        if (ConfigurationManager.getConfigInstance().getBoolean("netflix.genie.server.lipstick.enable", false)) {
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

            // try exact version first
            hadoopHome = ConfigurationManager
                    .getConfigInstance()
                    .getString("netflix.genie.server.hadoop." + hadoopVersion + ".home");
            // if not, trim to 3 most significant digits
            if (hadoopHome == null) {
                hadoopVersion = StringUtil.trimVersion(hadoopVersion);
                hadoopHome = ConfigurationManager.getConfigInstance()
                        .getString("netflix.genie.server.hadoop." + hadoopVersion + ".home");
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
                    .getString("netflix.genie.server.hadoop.home");
            if (hadoopHome == null || !new File(hadoopHome).exists()) {
                final String msg = "Property netflix.genie.server.hadoop.home is not set correctly";
                LOG.error(msg);
                throw new GenieServerException(msg);
            }
            processEnv.put("HADOOP_HOME", hadoopHome);
        }

        // populate the CP timeout and other options. Yarn jobs would use
        // hadoop fs -cp to copy files. Prepare the copy command with the combination
        // and set the COPY_COMMAND environment variable
        processEnv.put("CP_TIMEOUT",
                ConfigurationManager.getConfigInstance()
                        .getString("netflix.genie.server.hadoop.s3cp.timeout", "1800"));

        final String cpOpts = ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.hadoop.s3cp.opts", "");

        final String copyCommand = hadoopHome + "/bin/hadoop fs " + cpOpts + " -cp -f";
        processEnv.put("COPY_COMMAND", copyCommand);

        // Force flag to overwrite required in Hadoop2
        processEnv.put("FORCE_COPY_FLAG", "-f");

        final String mkdirCommand = hadoopHome + "/bin/hadoop fs " + cpOpts + " -mkdir";
        processEnv.put("MKDIR_COMMAND", mkdirCommand);
    }
}
