/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.agent.execution.process.impl;

import com.google.common.collect.Lists;
import com.netflix.genie.agent.cli.UserConsole;
import com.netflix.genie.agent.execution.exceptions.JobLaunchException;
import com.netflix.genie.agent.execution.process.JobProcessManager;
import com.netflix.genie.agent.execution.process.JobProcessResult;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.utils.EnvUtils;
import com.netflix.genie.agent.utils.PathUtils;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.scheduling.TaskScheduler;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Configures and launches a job sub-process using metadata passed through ExecutionContext.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class JobProcessManagerImpl implements JobProcessManager {

    private static final int SUCCESS_EXIT_CODE = 0;
    private final AtomicBoolean launched = new AtomicBoolean(false);
    private final AtomicReference<Process> processReference = new AtomicReference<>();
    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final AtomicReference<KillService.KillSource> killSource = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture> timeoutKillThread = new AtomicReference<>();

    private final TaskScheduler taskScheduler;

    /**
     * Constructor.
     *
     * @param taskScheduler The {@link TaskScheduler} instance to use to run scheduled asynchronous tasks
     */
    public JobProcessManagerImpl(final TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void launchProcess(
        final File jobDirectory,
        final Map<String, String> environmentVariablesMap,
        final List<String> commandArguments,
        final List<String> jobArguments,
        final boolean interactive,
        @Nullable final Integer timeout,
        final boolean launchInJobDirectory
    ) throws JobLaunchException {
        if (!this.launched.compareAndSet(false, true)) {
            throw new IllegalStateException("Job already launched");
        }

        final ProcessBuilder processBuilder = new ProcessBuilder();

        // Validate job running directory
        if (jobDirectory == null) {
            throw new JobLaunchException("Job directory is null");
        } else if (!jobDirectory.exists()) {
            throw new JobLaunchException("Job directory does not exist: " + jobDirectory);
        } else if (!jobDirectory.isDirectory()) {
            throw new JobLaunchException("Job directory is not a directory: " + jobDirectory);
        } else if (!jobDirectory.canWrite()) {
            throw new JobLaunchException("Job directory is not writable: " + jobDirectory);
        }

        final Map<String, String> currentEnvironmentVariables = processBuilder.environment();

        if (environmentVariablesMap == null) {
            throw new JobLaunchException("Job environment variables map is null");
        }

        // Merge job environment variables into process inherited environment
        environmentVariablesMap.forEach((key, value) -> {
            final String replacedValue = currentEnvironmentVariables.put(key, value);
            if (StringUtils.isBlank(replacedValue)) {
                log.debug(
                    "Added job environment variable: {}={}",
                    key,
                    value
                );
            } else if (!replacedValue.equals(value)) {
                log.debug(
                    "Set job environment variable: {}={} (previous value: {})",
                    key,
                    value,
                    replacedValue
                );
            }
        });

        // Validate arguments
        if (commandArguments == null) {
            throw new JobLaunchException("Job command-line arguments is null");
        } else if (commandArguments.isEmpty()) {
            throw new JobLaunchException("Job command-line arguments are empty");
        }

        final List<String> commandLine = Lists.newArrayList(commandArguments);
        commandLine.addAll(jobArguments);

        log.info(
            "Job command-line: {} arguments: {} (working directory: {})",
            Arrays.toString(commandArguments.toArray()),
            Arrays.toString(jobArguments.toArray()),
            launchInJobDirectory ? jobDirectory : Paths.get("").toAbsolutePath().normalize().toString()
        );

        // Configure arguments
        final List<String> expandedCommandArguments;
        try {
            expandedCommandArguments = expandCommandLineVariables(
                commandArguments,
                Collections.unmodifiableMap(currentEnvironmentVariables)
            );
        } catch (final EnvUtils.VariableSubstitutionException e) {
            throw new JobLaunchException("Command executable and arguments variables could not be expanded", e);
        }

        final List<String> expandedCommandLine = Lists.newArrayList();
        expandedCommandLine.addAll(expandedCommandArguments);
        expandedCommandLine.addAll(jobArguments);

        log.info("Command-line after expansion: {}", expandedCommandLine);

        processBuilder.command(expandedCommandLine);

        if (launchInJobDirectory) {
            processBuilder.directory(jobDirectory);
        }

        if (interactive) {
            processBuilder.inheritIO();
        } else {
            processBuilder.redirectError(PathUtils.jobStdErrPath(jobDirectory).toFile());
            processBuilder.redirectOutput(PathUtils.jobStdOutPath(jobDirectory).toFile());
        }

        if (this.killed.get()) {
            log.info("Job aborted, skipping launch");
        } else {
            log.info("Launching job");
            try {
                this.processReference.set(processBuilder.start());
                if (timeout != null) {
                    // NOTE: There is a chance of a SLIGHT delay here between the process launch and the timeout
                    final Instant timeoutInstant = Instant.now().plusSeconds(timeout);
                    this.timeoutKillThread.set(
                        this.taskScheduler.schedule(new TimeoutKiller(this), timeoutInstant)
                    );
                    log.info("Scheduled timeout kill to occur {} second(s) from now at {}", timeout, timeoutInstant);
                }
            } catch (final IOException | SecurityException e) {
                throw new JobLaunchException("Failed to launch job: ", e);
            }
            log.info("Process launched (pid: {})", this.getPid(this.processReference.get()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void kill(final KillService.KillSource source) {
        // TODO: These may need to be done atomically as a tandem
        if (!this.killed.compareAndSet(false, true)) {
            // this job was already killed by something else
            return;
        }
        this.killSource.set(source);

        final Process process = this.processReference.get();

        // TODO: This probably isn't enough. We need retries with a force at the end
        if (process != null) {
            process.destroy();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobProcessResult waitFor() throws InterruptedException {
        if (!this.launched.get()) {
            throw new IllegalStateException("Process not launched");
        }

        final Process process = this.processReference.get();

        int exitCode = 0;
        if (process != null) {
            exitCode = process.waitFor();
            UserConsole.getLogger().info("Job process terminated with exit code: {}", exitCode);
        }

        try {
            // Evil-but-necessary little hack.
            // The agent and the child job process receive SIGINT at the same time (e.g. in case of ctrl-c).
            // If the child terminates quickly, the code below will execute before the signal handler has a chance to
            // set the job as killed, and the final status would be (incorrectly) reported as success/failure,
            // depending on exit code, as opposed to killed.
            // So give the handler a chance to raise the 'killed' flag before attempting to read it.
            Thread.sleep(100);
        } catch (final InterruptedException e) {
            // Do nothing.
        }

        // If for whatever reason the timeout thread is currently running or if it is scheduled to be run, cancel it
        final ScheduledFuture timeoutThreadFuture = this.timeoutKillThread.get();
        if (timeoutThreadFuture != null) {
            timeoutThreadFuture.cancel(true);
        }

        // TODO: This doesn't seem quite right to me and is confirmed by an existing test. If the job completes
        //       successfully but then the system calls kill before this method then the final job status will be
        //       killed instead of successful. We should discuss this - TJG 9/9/2019
        if (this.killed.get()) {
            final KillService.KillSource source = this.killSource.get() != null
                ? this.killSource.get()
                : KillService.KillSource.API_KILL_REQUEST;

            switch (source) {
                case TIMEOUT:
                    return new JobProcessResult
                        .Builder(JobStatus.KILLED, JobStatusMessages.JOB_EXCEEDED_TIMEOUT, exitCode)
                        .build();
                case API_KILL_REQUEST:
                case SYSTEM_SIGNAL:
                default:
                    return new JobProcessResult
                        .Builder(JobStatus.KILLED, JobStatusMessages.JOB_KILLED_BY_USER, exitCode)
                        .build();
            }
        } else if (exitCode == SUCCESS_EXIT_CODE) {
            return new JobProcessResult.Builder(
                JobStatus.SUCCEEDED,
                JobStatusMessages.JOB_FINISHED_SUCCESSFULLY,
                exitCode
            ).build();
        } else {
            return new JobProcessResult.Builder(JobStatus.FAILED, JobStatusMessages.JOB_FAILED, exitCode).build();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onApplicationEvent(final KillService.KillEvent event) {
        final KillService.KillSource source = event.getKillSource();
        log.info("Stopping state machine due to kill event (source: {})", source);
        this.kill(source);
    }

    private List<String> expandCommandLineVariables(
        final List<String> commandLine,
        final Map<String, String> environmentVariables
    ) throws EnvUtils.VariableSubstitutionException {
        final ArrayList<String> expandedCommandLine = new ArrayList<>(commandLine.size());

        for (final String argument : commandLine) {
            expandedCommandLine.add(
                EnvUtils.expandShellVariables(argument, environmentVariables)
            );
        }

        return Collections.unmodifiableList(expandedCommandLine);
    }

    /* TODO: HACK, Process does not expose PID in Java 8 API */
    private long getPid(final Process process) {
        long pid = -1;
        final String processClassName = process.getClass().getCanonicalName();
        try {
            if ("java.lang.UNIXProcess".equals(processClassName)) {
                final Field pidMemberField = process.getClass().getDeclaredField("pid");
                final boolean resetAccessible = pidMemberField.isAccessible();
                pidMemberField.setAccessible(true);
                pid = pidMemberField.getLong(process);
                pidMemberField.setAccessible(resetAccessible);
            } else {
                log.debug("Don't know how to access PID for class {}", processClassName);
            }
        } catch (final Throwable t) {
            log.warn("Failed to determine job process PID");
        }
        return pid;
    }

    /**
     * This class is meant to be run in a thread that wakes up after some period of time and initiates a kill of the
     * job process due to the job timing out.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Slf4j
    private static class TimeoutKiller implements Runnable {
        private final JobProcessManager jobProcessManager;

        TimeoutKiller(final JobProcessManager jobProcessManager) {
            this.jobProcessManager = jobProcessManager;
        }

        /**
         * When this thread is run it is expected that the timeout duration has been reached so the run merely
         * sends a kill signal to the manager.
         */
        @Override
        public void run() {
            log.info("Timeout for job reached at {}. Sending kill signal to terminate job.", Instant.now());
            this.jobProcessManager.kill(KillService.KillSource.TIMEOUT);
        }
    }
}
