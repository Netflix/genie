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

package com.netflix.genie.agent.execution;

import com.netflix.genie.agent.execution.statemachine.actions.StateAction;
import com.netflix.genie.common.dto.v4.JobSpecification;

import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotBlank;
import java.io.File;
import java.util.Deque;
import java.util.Map;

/**
 * Stateful context used by execution components to track state.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ThreadSafe
public interface ExecutionContext {

    /**
     * Get the agent identifier.
     *
     * @return a non-blank string if the agent identifier was set, or null.
     */
    String getAgentId();

    /**
     * Set the unique agent identifier obtained by the server.
     *
     * @param agentId a non-blank string
     * @throws RuntimeException if the agent id is already set
     */
    void setAgentId(@NotBlank final String agentId);

    /**
     * Set the job process once it has been launched.
     *
     * @param jobProcess a process handle for the children job
     */
    void setJobProcess(final Process jobProcess);

    /**
     * Get the job process.
     *
     * @return a Process, if it was set, or null
     */
    Process getJobProcess();

    /**
     * Get the job run directory.
     *
     * @return the job directory File if one was set up, or null
     */
    File getJobDirectory();

    /**
     * Set the job directory.
     *
     * @param jobDirectory the job directory
     */
    void setJobDirectory(final File jobDirectory);

    /**
     * Set the job specification.
     * @param jobSpecification the job specification
     */
    void setJobSpecification(final JobSpecification jobSpecification);

    /**
     * Get the job specification.
     *
     * @return the job specification if it was set, or null
     */
    JobSpecification getJobSpecification();

    /**
     * Get the environment variables map for the job process.
     *
     * @return a map of environment variables and values if one was set, or null
     */
    Map<String, String> getJobEnvironment();

    /**
     * Set the job environment variables map.
     *
     * @param jobEnvironment a map of environment variables and their value to be passed to the job process at launch
     */
    void setJobEnvironment(final Map<String, String> jobEnvironment);

    /**
     * Get the queue of states visited for the purpose of tracking post-job cleanup execution.
     * @return a deque of state actions executed
     */
    Deque<StateAction> getCleanupActions();
}
