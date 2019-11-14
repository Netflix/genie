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
package com.netflix.genie.web.jobs.workflow.impl;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.jobs.JobExecutionEnvironment;
import com.netflix.genie.web.util.MetricsUtils;
import com.netflix.genie.web.util.UNIXUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the workflow task for processing job information for genie mode.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class JobKickoffTask extends GenieBaseTask {

    private static final String JOB_KICKOFF_TASK_TIMER_NAME = "genie.jobs.tasks.jobKickoffTask.timer";
    private final boolean isRunAsUserEnabled;
    private final boolean isUserCreationEnabled;
    private final Executor executor;
    private final String hostname;
    private final RetryTemplate retryTemplate;

    /**
     * Constructor.
     *
     * @param runAsUserEnabled    Flag that tells if job should be run as user specified in the request
     * @param userCreationEnabled Flag that tells if the user specified should be created
     * @param executor            An executor object used to run jobs
     * @param hostname            Hostname for the node the job is running on
     * @param registry            The metrics registry to use
     */
    public JobKickoffTask(
        final boolean runAsUserEnabled,
        final boolean userCreationEnabled,
        @NotNull final Executor executor,
        @NotNull final String hostname,
        @NotNull final MeterRegistry registry
    ) {
        super(registry);
        this.isRunAsUserEnabled = runAsUserEnabled;
        this.isUserCreationEnabled = userCreationEnabled;
        this.executor = executor;
        this.hostname = hostname;
        this.retryTemplate = new RetryTemplate();
        this.retryTemplate.setBackOffPolicy(new ExponentialBackOffPolicy());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(@NotNull final Map<String, Object> context) throws GenieException, IOException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final JobExecutionEnvironment jobExecEnv =
                (JobExecutionEnvironment) context.get(JobConstants.JOB_EXECUTION_ENV_KEY);
            final String jobWorkingDirectory = jobExecEnv.getJobWorkingDir().getCanonicalPath();
            final JobRequest jobRequest = jobExecEnv.getJobRequest();
            final String user = jobRequest.getUser();
            final Writer writer = (Writer) context.get(JobConstants.WRITER_KEY);
            final String jobId = jobRequest
                .getId()
                .orElseThrow(() -> new GeniePreconditionException("No job id found. Unable to continue."));
            log.info("Starting Job Kickoff Task for job {}", jobId);

            // At this point all contents are written to the run script and we call an explicit flush and close to write
            // the contents to the file before we execute it.
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                throw new GenieServerException("Failed to execute job", e);
            }
            // Create user, if enabled
            if (isUserCreationEnabled) {
                try {
                    UNIXUtils.createUser(user, jobRequest.getGroup().orElse(null), executor);
                } catch (IOException ioexception) {
                    throw new GenieServerException("Could not create user " + user, ioexception);
                }
            }
            final List<String> command = new ArrayList<>();

            // If the OS is linux use setsid to launch the process so that the entire process tree
            // is launched in process group id which is the same as the pid of the parent process
            if (SystemUtils.IS_OS_LINUX) {
                command.add("setsid");
            }

            // Set the ownership to the user and run as the user, if enabled
            if (isRunAsUserEnabled) {
                try {
                    UNIXUtils.changeOwnershipOfDirectory(jobWorkingDirectory, user, executor);
                } catch (IOException ioexception) {
                    throw new GenieServerException("Could not change ownership", ioexception);
                }

                // This is needed because the genie.log file is still generated as the user running Genie system.
                try {
                    UNIXUtils.makeDirGroupWritable(jobWorkingDirectory + "/genie/logs", executor);
                } catch (IOException e) {
                    throw new GenieServerException("Could not make the job working logs directory group writable.", e);
                }
                command.add("sudo");
                command.add("-u");
                command.add(user);
            }

            // Launch via bash rather than executing the run script directly to avoid 'Text file busy' error.
            // https://bugs.openjdk.java.net/browse/JDK-8068370
            command.add("bash");

            final String runScript = jobWorkingDirectory
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_JOB_LAUNCHER_SCRIPT;
            command.add(runScript);

            // Cannot convert to executor because it does not provide an api to get process id.
            final ProcessBuilder pb = new ProcessBuilder(command)
                .directory(jobExecEnv.getJobWorkingDir())
                .redirectOutput(new File(jobExecEnv.getJobWorkingDir() + JobConstants.GENIE_LOG_PATH))
                .redirectError(new File(jobExecEnv.getJobWorkingDir() + JobConstants.GENIE_LOG_PATH));

            try {
                final Process process = pb.start();
                final int processId = this.getProcessId(process);
                final Instant timeout = Instant
                    .now()
                    .plus(jobRequest.getTimeout().orElse(JobRequest.DEFAULT_TIMEOUT_DURATION), ChronoUnit.SECONDS);
                final JobExecution jobExecution = new JobExecution
                    .Builder(this.hostname)
                    .withId(jobId)
                    .withProcessId(processId)
                    .withCheckDelay(jobExecEnv.getCommand().getCheckDelay())
                    .withTimeout(timeout)
                    .withMemory(jobExecEnv.getMemory())
                    .build();
                context.put(JobConstants.JOB_EXECUTION_DTO_KEY, jobExecution);
            } catch (final IOException ie) {
                throw new GenieServerException("Unable to start command " + command, ie);
            }
            log.info("Finished Job Kickoff Task for job {}", jobId);
            MetricsUtils.addSuccessTags(tags);
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.getRegistry()
                .timer(JOB_KICKOFF_TASK_TIMER_NAME, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Helper method  to get process id for the given process.
     *
     * @param proc java process object representing the job launcher
     * @return pid for this process
     * @throws GenieException if there is an error getting the process id
     */
    private int getProcessId(final Process proc) throws GenieException {
        log.debug("called");

        try {
            final Field f = proc.getClass().getDeclaredField(JobConstants.PID);
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
