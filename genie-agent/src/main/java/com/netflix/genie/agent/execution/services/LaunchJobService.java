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

package com.netflix.genie.agent.execution.services;

import com.netflix.genie.agent.execution.exceptions.JobLaunchException;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Service that launches the job process.
 * @author mprimi
 * @since 4.0.0
 */
public interface LaunchJobService {

    /**
     * Launch the job process.
     * @return a Process object
     * @throws JobLaunchException if the job process failed to launch
     * @param runDirectory Run directory
     * @param environmentVariables additional environment variables (to merge on top of inherited environment)
     * @param commandLine command-line executable and arguments
     * @param interactive launch in interactive mode (inherit I/O) or batch (no input, write outputs to files)
     */
    Process launchProcess(
        final File runDirectory,
        final Map<String, String> environmentVariables,
        final List<String> commandLine,
        final boolean interactive
    ) throws JobLaunchException;
}
