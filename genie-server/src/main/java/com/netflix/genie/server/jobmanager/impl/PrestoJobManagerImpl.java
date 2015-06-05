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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Implementation of job manager for Presto Jobs.
 *
 * @author tgianos
 */
public class PrestoJobManagerImpl extends JobManagerImpl {

    private static final Logger LOG = LoggerFactory.getLogger(PrestoJobManagerImpl.class);
    private static final String PRESTO_PROTOCOL_KEY = "com.netflix.genie.server.job.manager.presto.protocol";
    private static final String PRESTO_MASTER_DOMAIN = "com.netflix.genie.server.job.manager.presto.master.domain";
    private static final String COPY_COMMAND_KEY = "com.netflix.genie.server.job.manager.presto.command.cp";
    private static final String MK_DIRECTORY_COMMAND_KEY = "com.netflix.genie.server.job.manager.presto.command.mkdir";

    /**
     * Constructor.
     *
     * @param jobMonitor     The job monitor object to use.
     * @param jobService     The job service to use.
     * @param commandService The command service to use.
     */
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

        // Check the parameters
        final String prestoProtocol = ConfigurationManager
                .getConfigInstance().getString(PRESTO_PROTOCOL_KEY, null);
        if (prestoProtocol == null) {
            throw new GeniePreconditionException("Presto protocol not set. Please configure " + PRESTO_PROTOCOL_KEY);
        }
        final String prestoMasterDomain = ConfigurationManager
                .getConfigInstance().getString(PRESTO_MASTER_DOMAIN, null);
        if (prestoMasterDomain == null) {
            throw new GeniePreconditionException("Presto protocol not set. Please configure " + PRESTO_MASTER_DOMAIN);
        }


        // create the ProcessBuilder for this process
        final List<String> processArgs = this.createBaseProcessArguments();
        processArgs.add("--server");
        processArgs.add(prestoProtocol + this.getCluster().getName() + prestoMasterDomain);
        processArgs.add("--catalog");
        processArgs.add("hive");
        processArgs.add("--user");
        processArgs.add(this.getJob().getUser());
        processArgs.add("--debug");
        processArgs.addAll(Arrays.asList(StringUtil.splitCmdLine(this.getJob().getCommandArgs())));

        final ProcessBuilder processBuilder = new ProcessBuilder(processArgs);

        // construct the environment variables
        this.setupCommonProcess(processBuilder);
        this.setupPrestoProcess(processBuilder);

        // Launch the actual process
        this.launchProcess(
                processBuilder,
                ConfigurationManager
                        .getConfigInstance()
                        .getInt("com.netflix.genie.server.job.manager.presto.sleeptime", 5000)
        );
    }

    /**
     * Set up the process with specific Presto properties.
     *
     * @param processBuilder The process builder to modify.
     * @throws GenieException If there are any issues.
     */
    private void setupPrestoProcess(final ProcessBuilder processBuilder) throws GenieException {
        final Map<String, String> processEnv = processBuilder.environment();

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

        final String makeDirCommand =
                ConfigurationManager.getConfigInstance()
                        .getString(MK_DIRECTORY_COMMAND_KEY);
        if (StringUtils.isBlank(makeDirCommand)) {
            final String msg = "Required property " + MK_DIRECTORY_COMMAND_KEY + " isn't set";
            LOG.error(msg);
            throw new GenieServerException(msg);
        }
        processEnv.put("MKDIR_COMMAND", makeDirCommand);
    }
}
