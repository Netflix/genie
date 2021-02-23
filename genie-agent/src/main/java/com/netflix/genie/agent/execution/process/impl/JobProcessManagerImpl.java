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

import com.netflix.genie.agent.cli.logging.ConsoleLog;
import com.netflix.genie.agent.execution.exceptions.JobLaunchException;
import com.netflix.genie.agent.execution.process.JobProcessManager;
import com.netflix.genie.agent.execution.process.JobProcessResult;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.utils.PathUtils;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.scheduling.TaskScheduler;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
    private final AtomicReference<File> initFailedFileRef = new AtomicReference<>();
    private Boolean isInteractiveMode;

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
        final File jobScript,
        final boolean interactive,
        @Nullable final Integer timeout,
        final boolean launchInJobDirectory) throws JobLaunchException {
        if (!this.launched.compareAndSet(false, true)) {
            throw new IllegalStateException("Job already launched");
        }

        this.isInteractiveMode = interactive;

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

        if (jobScript == null) {
            throw new JobLaunchException("Job script is null");
        } else if (!jobScript.exists() || !jobScript.isFile()) {
            throw new JobLaunchException("Job script is not a valid file");
        } else if (!jobScript.canExecute()) {
            throw new JobLaunchException("Job script is not executable");
        }

        this.initFailedFileRef.set(PathUtils.jobSetupErrorMarkerFilePath(jobDirectory).toFile());

        log.info("Executing job script: {} (working directory: {})",
            jobScript.getAbsolutePath(),
            launchInJobDirectory ? jobDirectory : Paths.get("").toAbsolutePath().normalize().toString());

        processBuilder.command(jobScript.getAbsolutePath());

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
            return;
        }
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
        } catch (IOException | SecurityException e) {
            throw new JobLaunchException("Failed to launch job: ", e);
        }
        log.info("Process launched (pid: {})", this.getPid(this.processReference.get()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void kill(final KillService.KillSource source) {
        log.info("Killing job process (kill event source: {})", source);
        if (!this.killed.compareAndSet(false, true)) {
            // this job was already killed by something else
            return;
        }
        this.killSource.set(source);
        final Process process = this.processReference.get();
        if (process == null) {
            return;
        }

        try {
            // Grace killing period
            gracefullyKill(process);
            if (!process.isAlive()) {
                log.info("Gracefully killed job process successfully");
                return;
            }
            log.info("Forcefully killing job process");
            forcefullyKill(process);
            if (!process.isAlive()) {
                log.info("Forcefully killed job process successfully");
                return;
            }
        } catch (Throwable t) {
            log.warn("Process kill with exception: {}", t.getMessage());
        }
        log.warn("Failed to kill job process");
    }

    private void gracefullyKill(final Process process) throws Exception {
        final Instant graceKillEnd = Instant.now().plusSeconds(10);
        process.destroy();
        while (process.isAlive() && Instant.now().isBefore(graceKillEnd)) {
            process.waitFor(100, TimeUnit.MILLISECONDS);
        }
    }

    private void forcefullyKill(final Process process) throws Exception {
        final Instant forceKillEnd = Instant.now().plusSeconds(10);
        // In Java8, this is exactly destroy(). However, this behavior can be changed in future java.
        process.destroyForcibly();
        while (process.isAlive() && Instant.now().isBefore(forceKillEnd)) {
            process.waitFor(100, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobProcessResult waitFor() throws InterruptedException {
        if (!launched.get()) {
            throw new IllegalStateException("Process not launched");
        }

        final Process process = processReference.get();

        int exitCode = 0;
        if (process != null) {
            exitCode = process.waitFor();
            ConsoleLog.getLogger().info("Job process terminated with exit code: {}", exitCode);
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

        // Check exit code first to see if the job finishes successfully and returns SUCCEEDED as status,
        // even the job gets a KILL request.
        if (process != null && exitCode == SUCCESS_EXIT_CODE) {
            return new JobProcessResult.Builder(
                JobStatus.SUCCEEDED,
                JobStatusMessages.JOB_FINISHED_SUCCESSFULLY,
                exitCode
            ).build();
        }

        if (this.killed.get()) {
            final KillService.KillSource source = ObjectUtils.firstNonNull(
                killSource.get(), KillService.KillSource.API_KILL_REQUEST);
            switch (source) {
                case TIMEOUT:
                    return new JobProcessResult
                        .Builder(JobStatus.KILLED, JobStatusMessages.JOB_EXCEEDED_TIMEOUT, exitCode)
                        .build();
                case FILES_LIMIT:
                    return new JobProcessResult
                        .Builder(JobStatus.KILLED, JobStatusMessages.JOB_EXCEEDED_FILES_LIMIT, exitCode)
                        .build();
                case REMOTE_STATUS_MONITOR:
                    return new JobProcessResult
                        .Builder(JobStatus.KILLED, JobStatusMessages.JOB_MARKED_FAILED, exitCode)
                        .build();
                case SYSTEM_SIGNAL:
                    // In interactive mode, killed by a system signal is mostly likely by a user (e.g. Ctrl-C)
                    return new JobProcessResult
                        .Builder(JobStatus.KILLED,
                        isInteractiveMode
                        ? JobStatusMessages.JOB_KILLED_BY_USER : JobStatusMessages.JOB_KILLED_BY_SYSTEM,
                        exitCode)
                        .build();
                case API_KILL_REQUEST:
                default:
                    return new JobProcessResult
                        .Builder(JobStatus.KILLED, JobStatusMessages.JOB_KILLED_BY_USER, exitCode)
                        .build();
            }
        }

        final File initFailedFile = initFailedFileRef.get();
        final String statusMessage = (initFailedFile != null && initFailedFile.exists())
            ? JobStatusMessages.JOB_SETUP_FAILED : JobStatusMessages.JOB_FAILED;

        return new JobProcessResult.Builder(JobStatus.FAILED, statusMessage, exitCode).build();
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
