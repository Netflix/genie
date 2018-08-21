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

package com.netflix.genie.agent.execution.services.impl;

import com.netflix.genie.agent.execution.exceptions.JobLaunchException;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.services.LaunchJobService;
import com.netflix.genie.agent.utils.EnvUtils;
import com.netflix.genie.agent.utils.PathUtils;
import com.netflix.genie.common.dto.JobStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Configures and launches a job sub-process using metadata passed through ExecutionContext.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class LaunchJobServiceImpl implements LaunchJobService {

    private final AtomicBoolean launched = new AtomicBoolean(false);
    private final AtomicReference<Process> processReference = new AtomicReference<>();
    private final AtomicBoolean killed = new AtomicBoolean(false);

    /**
     * {@inheritDoc}
     */
    @Override
    public void launchProcess(
        final File runDirectory,
        final Map<String, String> environmentVariablesMap,
        final List<String> commandLine,
        final boolean interactive
    ) throws JobLaunchException {

        if (!launched.compareAndSet(false, true)) {
            throw new IllegalStateException("Job already launched");
        }

        final ProcessBuilder processBuilder = new ProcessBuilder();

        // Validate job running directory
        if (runDirectory == null) {
            throw new JobLaunchException("Job directory is null");
        } else if (!runDirectory.exists()) {
            throw new JobLaunchException("Job directory does not exist: " + runDirectory);
        } else if (!runDirectory.isDirectory()) {
            throw new JobLaunchException("Job directory is not a directory: " + runDirectory);
        } else if (!runDirectory.canWrite()) {
            throw new JobLaunchException("Job directory is not writable: " + runDirectory);
        }

        // Configure job running directory
        processBuilder.directory(runDirectory);

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
        if (commandLine == null) {
            throw new JobLaunchException("Job command-line arguments is null");
        } else if (commandLine.isEmpty()) {
            throw new JobLaunchException("Job command-line arguments are empty");
        }

        // Configure arguments
        log.info("Job command-line: {}", Arrays.toString(commandLine.toArray()));

        final List<String> expandedCommandLine;
        try {
            expandedCommandLine = expandCommandLineVariables(
                commandLine,
                Collections.unmodifiableMap(currentEnvironmentVariables)
            );
        } catch (final EnvUtils.VariableSubstitutionException e) {
            throw new JobLaunchException("Job command-line arguments variables could not be expanded");
        }

        if (!commandLine.equals(expandedCommandLine)) {
            log.info("Job command-line with variables expanded: {}", Arrays.toString(expandedCommandLine.toArray()));
        }

        processBuilder.command(expandedCommandLine);

        if (interactive) {
            processBuilder.inheritIO();
        } else {
            processBuilder.redirectError(PathUtils.jobStdErrPath(runDirectory).toFile());
            processBuilder.redirectOutput(PathUtils.jobStdOutPath(runDirectory).toFile());
        }

        if (killed.get()) {
            log.info("Job aborted, skipping launch");
        } else {
            log.info("Launching job");
            try {
                processReference.set(processBuilder.start());
            } catch (final IOException | SecurityException e) {
                throw new JobLaunchException("Failed to launch job: ", e);
            }
            log.info("Process launched (pid: {})", getPid(processReference.get()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void kill() {
        killed.set(true);

        final Process process = processReference.get();

        if (process != null) {
            process.destroy();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatus waitFor() throws InterruptedException {

        if (!launched.get()) {
            throw new IllegalStateException("Process not launched");
        }

        final Process process = processReference.get();

        int exitCode = 0;
        if (process != null) {
            exitCode = process.waitFor();
            log.info("Job process completed with exit code: {}", exitCode);
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

        if (killed.get()) {
            return JobStatus.KILLED;
        } else if (exitCode == 0) {
            return JobStatus.SUCCEEDED;
        } else {
            return JobStatus.FAILED;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onApplicationEvent(final KillService.KillEvent event) {
        log.info("Stopping state machine due to kill event (source: {})", event.getKillSource());
        this.kill();
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
}


