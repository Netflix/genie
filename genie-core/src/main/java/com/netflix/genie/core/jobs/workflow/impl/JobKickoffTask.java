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
package com.netflix.genie.core.jobs.workflow.impl;

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the workflow task for processing job information for genie mode.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class JobKickoffTask extends GenieBaseTask {

    private boolean isRunAsUserEnabled;
    private boolean isUserCreationEnabled;
    private Executor executor;
    private String hostname;

    /**
     * Constructor.
     *
     * @param runAsUserEnabled Flag that tells if job should be run as user specified in the request
     * @param userCreationEnabled Flag that tells if the user specified should be created
     * @param executor An executor object used to run jobs
     * @param hostname Hostname for the node the job is running on
     */
    public JobKickoffTask(
        final boolean runAsUserEnabled,
        final boolean userCreationEnabled,
        final Executor executor,
        final String hostname
    ) {
        this.isRunAsUserEnabled = runAsUserEnabled;
        this.isUserCreationEnabled = userCreationEnabled;
        this.executor = executor;
        this.hostname = hostname;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException {
        log.info("Executing Job Kickoff Task in the workflow.");
        super.executeTask(context);

        final String runScript = this.jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_JOB_LAUNCHER_SCRIPT;

        if (this.isUserCreationEnabled) {
            createUser(this.jobExecEnv.getJobRequest().getUser(), this.jobExecEnv.getJobRequest().getGroup());
        }

        final List<String> command = new ArrayList<>();
        if (this.isRunAsUserEnabled) {
            changeOwnershipOfDirectory(this.jobWorkingDirectory, this.jobExecEnv.getJobRequest().getUser());
            command.add("sudo");
            command.add("-u");
            command.add(this.jobExecEnv.getJobRequest().getUser());
        }
        command.add("bash");
        command.add(runScript);

        // Cannot convert to executor because it does not provide an api to get process id.
        final ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(this.jobExecEnv.getJobWorkingDir());
        pb.redirectOutput(new File(this.jobExecEnv.getJobWorkingDir() + JobConstants.GENIE_LOG_PATH));
        pb.redirectError(new File(this.jobExecEnv.getJobWorkingDir() + JobConstants.GENIE_LOG_PATH));

        try {
            final Process process = pb.start();
            final int processId = Utils.getProcessId(process);
            final JobExecution jobExecution = new JobExecution
                .Builder(this.hostname, processId, this.jobExecEnv.getCommand().getCheckDelay())
                .withId(this.jobExecEnv.getJobRequest().getId())
                .build();
            context.put(JobConstants.JOB_EXECUTION_DTO_KEY, jobExecution);
        } catch (IOException ie) {
            throw new GenieServerException("Unable to start command " + String.valueOf(command), ie);
        }
    }

    /**
     * Create user on the system.
     *
     * @param user user id
     * @param group group id
     * @throws GenieException If there is any problem.
     */
    public void createUser(
        final String user,
        final String group) throws GenieException {

        // First check if user already exists
        final CommandLine idCheckCommandLine = new CommandLine("id");
        idCheckCommandLine.addArgument("-u");
        idCheckCommandLine.addArgument(user);

        try {
            executor.execute(idCheckCommandLine);
            log.info("User already exists");
        } catch (IOException ioe) {
            log.info("User does not exist. Creating it now.");
            final CommandLine userCreateCommandLine = new CommandLine("sudo");
            userCreateCommandLine.addArgument("useradd");
            userCreateCommandLine.addArgument(user);

            if (StringUtils.isNotBlank(group)) {
                userCreateCommandLine.addArgument("-G");
                userCreateCommandLine.addArgument(group);
            }

            userCreateCommandLine.addArgument("-M");

            try {
                executor.execute(userCreateCommandLine);
            } catch (IOException ioexception) {
                throw new GenieServerException("Could not create user " + user + "with exception " + ioexception);
            }
        }
    }

    /**
     * Method to change the ownership of a directory.
     *
     * @param dir The directory to change the ownership of.
     * @param user Userid of the user.
     * @throws GenieException If there is a problem.
     */
    public void changeOwnershipOfDirectory(
        final String dir,
        final String user) throws GenieException {

        final CommandLine commandLine = new CommandLine("chown");
        // Don;t need sudo probably as the genie user is the one creating the working directory.
        //command.add("sudo");
        //command.add("chown");
        commandLine.addArgument("-R");
        commandLine.addArgument(user);
        commandLine.addArgument(dir);

        try {
            executor.execute(commandLine);
        } catch (IOException ioexception) {
            throw new GenieServerException("Could not change ownership with exception " + ioexception);
        }
    }
}
