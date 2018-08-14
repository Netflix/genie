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

import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.agent.execution.statemachine.actions.StateAction;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.statemachine.action.Action;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotBlank;
import java.io.File;
import java.util.List;
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
     * Get the job run directory.
     *
     * @return the job directory File if one was set up, or null
     */
    @Nullable
    File getJobDirectory();

    /**
     * Set the job directory.
     *
     * @param jobDirectory the job directory
     */
    void setJobDirectory(final File jobDirectory);

    /**
     * Get the job specification.
     *
     * @return the job specification if it was set, or null
     */
    @Nullable
    JobSpecification getJobSpecification();

    /**
     * Set the job specification.
     *
     * @param jobSpecification the job specification
     */
    void setJobSpecification(final JobSpecification jobSpecification);

    /**
     * Get the environment variables map for the job process.
     *
     * @return a map of environment variables and values if one was set, or null
     */
    @Nullable
    Map<String, String> getJobEnvironment();

    /**
     * Set the job environment variables map.
     *
     * @param jobEnvironment a map of environment variables and their value to be passed to the job process at launch
     */
    void setJobEnvironment(final Map<String, String> jobEnvironment);

    /**
     * Enqueue cleanup for a state action.
     *
     * @param stateAction the action that needs cleanup
     */
    void addCleanupActions(StateAction stateAction);

    /**
     * Get the queue of states visited for the purpose of tracking post-job cleanup execution.
     * Actions are ordered in the order they were added (i.e., order in which the corresponding states were visited).
     *
     * @return a deque of state actions executed
     */
    List<StateAction> getCleanupActions();

    /**
     * Record a state action failure to execute and threw an exception.
     *
     * @param state       the state whose action failed with an exception
     * @param actionClass the class of the state action that failed
     * @param exception   the exception thrown by the state action
     */
    void addStateActionError(
        final States state,
        final Class<? extends Action> actionClass,
        final Exception exception
    );

    /**
     * Whether any state action executed so far failed.
     *
     * @return true if at least one state action failed with exception rather than completing successfully
     */
    boolean hasStateActionError();

    /**
     * Get the list of state actions that failed during execution, if any.
     *
     * @return a list of actions that failed in the form of a Triple.
     */
    List<Triple<States, Class<? extends Action>, Exception>> getStateActionErrors();

    /**
     * Get the final job status, if one was set.
     *
     * @return the final job status or null
     */
    @Nullable
    JobStatus getFinalJobStatus();

    /**
     * Set the final job status.
     *
     * @param jobStatus the final job status
     */
    void setFinalJobStatus(final JobStatus jobStatus);

    /**
     * Get the current job status.
     *
     * @return the latest job status set, or null if never set
     */
    @Nullable
    JobStatus getCurrentJobStatus();

    /**
     * Set the current job status.
     *
     * @param jobStatus the job status
     */
    void setCurrentJobStatus(final JobStatus jobStatus);

    /**
     * Get the job id, if a job was claimed.
     *
     * @return a job id or null a job was not claimed
     */
    @Nullable
    String getClaimedJobId();

    /**
     * Set the job id of a successfully claimed job.
     *
     * @param jobId the job id
     */
    void setClaimedJobId(@NotBlank String jobId);
}
