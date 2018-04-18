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

import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.agent.execution.statemachine.actions.StateAction;
import com.netflix.genie.common.dto.JobStatus;

import javax.annotation.Nullable;

/**
 * Service for publishing agent and job events.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface AgentEventsService {

    /**
     * Emit event due to job status change.
     * @param jobStatus the current job status
     */
    void emitJobStatusUpdate(final JobStatus jobStatus);

    /**
     * Emit event due to agent state machine state change.
     * @param fromState the previous state (can be null)
     * @param toState the next state
     */
    void emitStateChange(@Nullable final States fromState, final States toState);

    /**
     * Emit event due to StateAction successful completion.
     * @param state the current state
     * @param action the action executed
     */
    void emitStateActionExecution(final States state, final StateAction action);

    /**
     * Emit event due to failure during StateAction execution.
     * @param state the current state
     * @param action the action executed
     * @param exception the exception produced by the action
     */
    void emitStateActionExecution(
        final States state,
        final StateAction action,
        final Exception exception
    );
}
