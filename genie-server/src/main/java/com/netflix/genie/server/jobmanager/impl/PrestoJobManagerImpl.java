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
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.server.jobmanager.JobMonitor;
import com.netflix.genie.server.services.CommandConfigService;
import com.netflix.genie.server.services.JobService;
import com.netflix.genie.server.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Implementation of job manager for Presto Jobs.
 *
 * @author tgianos
 */
@Named
@Scope("prototype")
public class PrestoJobManagerImpl extends JobManagerImpl {

    private static final Logger LOG = LoggerFactory.getLogger(PrestoJobManagerImpl.class);

    /**
     * Constructor.
     *
     * @param jobMonitor     The job monitor object to use.
     * @param jobService     The job service to use.
     * @param commandService The command service to use.
     */
    @Inject
    public PrestoJobManagerImpl(final JobMonitor jobMonitor,
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
        processArgs.add("--server");
        processArgs.add("http://" + this.getCluster().getName() + ".master.dataeng.netflix.net:8080");
        processArgs.add("--catalog");
        processArgs.add("hive");
        processArgs.add("--debug");
        processArgs.addAll(Arrays.asList(StringUtil.splitCmdLine(this.getJob().getCommandArgs())));

        final ProcessBuilder processBuilder = new ProcessBuilder(processArgs);

        // construct the environment variables
        this.setupCommonProcess(processBuilder);
        this.setupPrestoProcess(processBuilder);

        // Launch the actual process
        this.launchProcess(processBuilder);
    }

    /**
     * Set up the process with specific Presto properties.
     *
     * @param processBuilder The process builder to modify.
     * @throws GenieException If there are any issues.
     */
    private void setupPrestoProcess(final ProcessBuilder processBuilder) throws GenieException {
        final Map<String, String> processEnv = processBuilder.environment();

        //Right now within netflix presto is run side by side with Hadoop EMR nodes so use Hadoop to copy
        //Files down from s3

        // set the default hadoop home
        final String hadoopHome = ConfigurationManager
                .getConfigInstance()
                .getString("netflix.genie.server.hadoop.home");
        if (hadoopHome == null || !new File(hadoopHome).exists()) {
            final String msg = "Property netflix.genie.server.hadoop.home is not set correctly";
            LOG.error(msg);
            throw new GenieServerException(msg);
        }
        processEnv.put("HADOOP_HOME", hadoopHome);

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
