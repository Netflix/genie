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
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationEventPublisher;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
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
public class JobKickoffTask extends GenieBaseTask implements WorkflowTask {

    private static final String  STDERR_LOG_PATH = "./job/stderr";
    private static final String STDOUT_LOG_PATH = "./job/stdout";
    private static final String GENIE_DONE_FILE = "./genie/genie.done";
    private static final String GENIE_LOG_PATH = "/genie/logs/genie.log";
    private static final String JOB_EXECUTION_DTO_KEY = "jexecdto";
    private static final String PID = "pid";
    private boolean isRunAsUserEnabled;
    private boolean isUserCreationEnabled;
    private ApplicationEventPublisher applicationEventPublisher;

    /**
     * Constructor.
     *
     * @param runAsUserEnabled Flag that tells if job should be run as user specified in the request
     * @param userCreationEnabled Flag that tells if the user specified should be created
     */
    public JobKickoffTask(
        final boolean runAsUserEnabled,
        final boolean userCreationEnabled
    ) {
        this.isRunAsUserEnabled = runAsUserEnabled;
        this.isUserCreationEnabled = userCreationEnabled;
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

        final JobExecutionEnvironment jobExecEnv =
            (JobExecutionEnvironment) context.get(JOB_EXECUTION_ENV_KEY);

        if (jobExecEnv == null) {
            throw new GenieServerException("Cannot run application task as jobExecutionEnvironment is null");
        }

        if (this.isUserCreationEnabled) {
            createUser(jobExecEnv.getJobRequest().getUser(), jobExecEnv.getJobRequest().getGroup());
        }

        final String jobLauncherScriptPath = jobExecEnv.getJobWorkingDir() + "/" + GENIE_JOB_LAUNCHER_SCRIPT;

        final List command = new ArrayList<>();
        if (this.isRunAsUserEnabled) {
            changeOwnershipOfDirectory(jobExecEnv.getJobWorkingDir(), jobExecEnv.getJobRequest().getUser());
            command.add("sudo");
            command.add("-u");
            command.add(jobExecEnv.getJobRequest().getUser());
        }
        command.add("bash");
        command.add(jobLauncherScriptPath);

        final ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(new File(jobExecEnv.getJobWorkingDir()));
        pb.redirectOutput(new File(jobExecEnv.getJobWorkingDir() + GENIE_LOG_PATH));
        pb.redirectError(new File(jobExecEnv.getJobWorkingDir() + GENIE_LOG_PATH));

        try {
            final Process process = pb.start();
            final String hostname = InetAddress.getLocalHost().getHostAddress();
            final int processId = getProcessId(process);
            final JobExecution jobExecution =
                new JobExecution.Builder(hostname, processId).withId(jobExecEnv.getJobRequest().getId()).build();
            context.put(JOB_EXECUTION_DTO_KEY, jobExecution);
        } catch (IOException ie) {
            throw new GenieServerException("Unable to start command " + String.valueOf(command), ie);
        }
    }

    private void createUser(
        final String user,
        final String group) throws GenieException {

        // First check if user already exists
        final List checkUserExistsCommand = new ArrayList<>();
        checkUserExistsCommand.add("id");
        checkUserExistsCommand.add("-u");
        checkUserExistsCommand.add(user);

        try {
            this.executeBashCommand(checkUserExistsCommand, null);
            log.info("User already exists");
            return;
        } catch (GenieException ge) {
            log.info("User does not exist. Creating it now.");
            final List command = new ArrayList<>();
            command.add("sudo");
            command.add("useradd");
            command.add(user);

            if (StringUtils.isNotBlank(group)) {
                command.add("-G");
                command.add(group);
            }

            command.add("-M");
            this.executeBashCommand(command, null);
        }
    }

    private void changeOwnershipOfDirectory(
        final String jobWorkingDir,
        final String user) throws GenieException {

        final List command = new ArrayList<>();
        // Don;t need sudo probably as the genie user is the one creating the working directory.
        //command.add("sudo");
        command.add("chown");
        command.add("-R");
        command.add(user);
        command.add(jobWorkingDir);

        this.executeBashCommand(command, null);
    }

    /**
     * Get process id for the given process.
     *
     * @param proc java process object representing the job launcher
     * @return pid for this process
     * @throws GenieException if there is an error getting the process id
     */
    private int getProcessId(final Process proc) throws GenieException {
        log.debug("called");

        try {
            final Field f = proc.getClass().getDeclaredField(PID);
            f.setAccessible(true);
            return f.getInt(proc);
        } catch (final IllegalAccessException
            | IllegalArgumentException
            | NoSuchFieldException
            | SecurityException e) {
            final String msg = "Can't get process id for job";
            log.error(msg, e);
            throw new GenieServerException(msg, e);
        }
    }
}
