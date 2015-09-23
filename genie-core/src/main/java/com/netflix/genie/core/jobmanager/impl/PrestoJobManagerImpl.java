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
package com.netflix.genie.core.jobmanager.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobmanager.JobMonitor;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.JobService;
import com.netflix.genie.core.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Implementation of job manager for Presto Jobs.
 *
 * @author tgianos
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrestoJobManagerImpl extends JobManagerImpl {

    private static final Logger LOG = LoggerFactory.getLogger(PrestoJobManagerImpl.class);
    private static final String PRESTO_PROTOCOL_KEY = "com.netflix.genie.server.job.manager.presto.protocol";
    private static final String PRESTO_MASTER_DOMAIN = "com.netflix.genie.server.job.manager.presto.master.domain";
    private static final String COPY_COMMAND_KEY = "com.netflix.genie.server.job.manager.presto.command.cp";
    private static final String MK_DIRECTORY_COMMAND_KEY = "com.netflix.genie.server.job.manager.presto.command.mkdir";

    @Value("${com.netflix.genie.server.job.manager.presto.protocol:null}")
    private String prestoProtocol;
    @Value("${com.netflix.genie.server.job.manager.presto.master.domain:null}")
    private String prestoMasterDomain;
    @Value("${com.netflix.genie.server.job.manager.presto.sleeptime:5000}")
    private int prestoSleepTime;
    @Value("${com.netflix.genie.server.hadoop.s3cp.timeout:1800}")
    private String copyTimeout;
    @Value("${com.netflix.genie.server.job.manager.presto.command.cp:null}")
    private String copyCommand;
    @Value("${com.netflix.genie.server.job.manager.presto.command.mkdir:null}")
    private String mkdirCommand;

    /**
     * Constructor.
     *
     * @param jobMonitor     The job monitor object to use.
     * @param jobService     The job service to use.
     * @param clusterService The cluster service to use.
     * @param commandService The command service to use.
     */
    @Autowired
    public PrestoJobManagerImpl(
            final JobMonitor jobMonitor,
            final JobService jobService,
            final ClusterService clusterService,
            final CommandService commandService
    ) {
        super(jobMonitor, jobService, clusterService, commandService);
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

        if (StringUtils.isBlank(this.prestoProtocol)) {
            throw new GeniePreconditionException("Presto protocol not set. Please configure " + PRESTO_PROTOCOL_KEY);
        }
        if (StringUtils.isBlank(this.prestoMasterDomain)) {
            throw new GeniePreconditionException("Presto protocol not set. Please configure " + PRESTO_MASTER_DOMAIN);
        }


        // create the ProcessBuilder for this process
        final List<String> processArgs = this.createBaseProcessArguments();
        processArgs.add("--server");
        processArgs.add(this.prestoProtocol + this.getCluster().getName() + this.prestoMasterDomain);
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
        this.launchProcess(processBuilder, this.prestoSleepTime);
    }

    /**
     * Set up the process with specific Presto properties.
     *
     * @param processBuilder The process builder to modify.
     * @throws GenieException If there are any issues.
     */
    private void setupPrestoProcess(final ProcessBuilder processBuilder) throws GenieException {
        final Map<String, String> processEnv = processBuilder.environment();

        processEnv.put("CP_TIMEOUT", this.copyTimeout);

        if (StringUtils.isBlank(this.copyCommand)) {
            throw new GenieServerException("Required property " + COPY_COMMAND_KEY + " isn't set");
        }
        processEnv.put("COPY_COMMAND", copyCommand);

        if (StringUtils.isBlank(mkdirCommand)) {
            throw new GenieServerException("Required property " + MK_DIRECTORY_COMMAND_KEY + " isn't set");
        }
        processEnv.put("MKDIR_COMMAND", mkdirCommand);
    }
}
