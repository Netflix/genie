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
import com.netflix.genie.core.jobs.workflow.Context;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Context context
    ) throws GenieException {
        log.info("Execution Job Task in the workflow.");

        final JobExecutionEnvironment jobExecEnv =
            (JobExecutionEnvironment) context.getAttribute(JOB_EXECUTION_ENV_KEY);

        if (jobExecEnv == null) {
            throw new GenieServerException("Cannot run application task as jobExecutionEnvironment is null");
        }

        final String jobLauncherScriptPath = jobExecEnv.getJobWorkingDir() + "/" + GENIE_JOB_LAUNCHER_SCRIPT;

        final List command = new ArrayList<>();
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
                new JobExecution.Builder(hostname, processId).withId(jobExecEnv.getId()).build();
            context.setAttribute(JOB_EXECUTION_DTO_KEY, jobExecution);
        } catch (IOException ie) {
            throw new GenieServerException("Unable to start command " + String.valueOf(command), ie);
        }
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
