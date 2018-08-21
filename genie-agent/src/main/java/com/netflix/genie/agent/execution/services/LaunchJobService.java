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
import com.netflix.genie.common.dto.JobStatus;
import org.springframework.context.ApplicationListener;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Service that launches the job process.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface LaunchJobService extends ApplicationListener<KillService.KillEvent> {

    /**
     * Launch the job process (unless launch was aborted by previous a {@code kill} call).
     *
     * @param runDirectory         Run directory
     * @param environmentVariables additional environment variables (to merge on top of inherited environment)
     * @param commandLine          command-line executable and arguments
     * @param interactive          launch in interactive mode (inherit I/O) or batch (no input, write outputs to files)
     * @throws JobLaunchException if the job process failed to launch
     */
    void launchProcess(
        final File runDirectory,
        final Map<String, String> environmentVariables,
        final List<String> commandLine,
        final boolean interactive
    ) throws JobLaunchException;

    /**
     * Terminate job process execution (if still running) or prevent it from launching (if not launched yet).
     * Optionally sends SIGINT to the process (unnecessary under certain circumstances. For example,
     * CTRL-C in a terminal session, is already received by the job process, issuing a second one is unneeded).
     *
     */
    void kill();

    /**
     * Wait indefinitely for the job process to terminate.
     *
     * @return KILLED, SUCCESSFUL, or FAILED
     * @throws IllegalStateException if the process was not launched
     * @throws InterruptedException  if the calling thread is interrupted while waiting
     */
    JobStatus waitFor() throws InterruptedException;
}
