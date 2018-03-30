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

import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.JobLaunchException;
import com.netflix.genie.agent.execution.services.LaunchJobService;
import com.netflix.genie.agent.utils.PathUtils;
import com.netflix.genie.common.dto.v4.JobSpecification;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

    private final ExecutionContext executionContext;

    LaunchJobServiceImpl(final ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Process launchProcess() throws JobLaunchException {
        final JobSpecification jobSpec = executionContext.getJobSpecification();

        if (jobSpec == null) {
            throw new JobLaunchException("Job specification is not set in execution context");
        }

        final ProcessBuilder processBuilder = new ProcessBuilder();

        configureEnvironment(processBuilder);
        configureCommandArguments(processBuilder, jobSpec);
        configureIO(processBuilder, jobSpec);

        try {
            return processBuilder.start();
        } catch (final IOException | SecurityException e) {
            throw new JobLaunchException("Failed to launch job: ", e);
        }
    }

    private void configureEnvironment(
        final ProcessBuilder processBuilder
    ) throws JobLaunchException {

        final File jobDirectory = executionContext.getJobDirectory();

        // Validate job running directory
        if (jobDirectory == null) {
            throw new JobLaunchException("Job directory is not set in execution context");
        } else if (!jobDirectory.exists()) {
            throw new JobLaunchException("Job directory does not exist: " + jobDirectory);
        } else if (!jobDirectory.isDirectory()) {
            throw new JobLaunchException("Job directory is not a directory: " + jobDirectory);
        } else if (!jobDirectory.canWrite()) {
            throw new JobLaunchException("Job directory is not writeable: " + jobDirectory);
        }

        // Configure job running directory
        processBuilder.directory(jobDirectory);

        final Map<String, String> currentEnvironmentVariables = processBuilder.environment();
        final Map<String, String> jobEnvironmentVariables = executionContext.getJobEnvironment();

        // Validate additional environment variables
        if (jobEnvironmentVariables == null) {
            throw new JobLaunchException("Job environment variables map is not set in execution context");
        }

        // Configure job environment variables
        jobEnvironmentVariables.forEach((key, value) -> {
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
    }

    private void configureCommandArguments(
        final ProcessBuilder processBuilder,
        final JobSpecification jobSpec
    ) throws JobLaunchException {
        final List<String> commandLineArguments = jobSpec.getCommandArgs();

        // Validate arguments
        if (commandLineArguments == null) {
            throw new JobLaunchException("Job command-line arguments not set in execution context");
        } else if (commandLineArguments.isEmpty()) {
            throw new JobLaunchException("Job command-line arguments are empty");
        }

        // Configure arguments
        log.info("Job command-line: {}", Arrays.toString(commandLineArguments.toArray()));
        processBuilder.command(commandLineArguments);
    }

    private void configureIO(
        final ProcessBuilder processBuilder,
        final JobSpecification jobSpec
    ) {
        if (jobSpec.isInteractive()) {
            processBuilder.inheritIO();
        } else {
            final File jobDirectory = executionContext.getJobDirectory();
            processBuilder.redirectError(PathUtils.jobStdErrPath(jobDirectory).toFile());
            processBuilder.redirectOutput(PathUtils.jobStdOutPath(jobDirectory).toFile());
        }
    }
}
