/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.agent.execution.statemachine;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract base class for Genie Agent stage of execution.
 * The execution state machine is constructed as a sequence of stages that each job goes through (e.g., claim job,
 * setup job directory, launch job, etc).
 * Each stage consists of a state machine state and an action that needs to be completed in order to successfully
 * transition to the next stage. This action is implemented in concrete subclasses.
 * <p>
 * Stages are categorized along these dimensions:
 * - Critical vs. optional: if a critical stage fails, execution is aborted and the job is considered failed.
 * Optional stages can produce fatal error without compromising the overall execution (example: job file archival).
 * - Skippable vs. non-skippable: skippable stages are skipped if a job was aborted due to fatal error or kill.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Getter
public abstract class ExecutionStage {
    private final States state;

    /**
     * Constructor.
     *
     * @param state the state machine state associated with this stage
     */
    protected ExecutionStage(final States state) {
        this.state = state;
    }

    protected FatalJobExecutionException createFatalException(final String message, final Throwable cause) {
        return new FatalJobExecutionException(this.getState(), message, cause);
    }

    protected FatalJobExecutionException createFatalException(final Throwable cause) {
        return this.createFatalException(
            "Fatal error in state " + this.getState().name() + ": " + cause.getMessage(),
            cause
        );
    }

    protected RetryableJobExecutionException createRetryableException(final Throwable cause) {
        throw new RetryableJobExecutionException(
            "Retryable error in state " + this.getState().name() + ": " + cause.getMessage(),
            cause
        );
    }

    /**
     * Action associated with this stage. May be skipped if execution is aborted and the stage is defined as skippable,
     * or if the action was attempted and threw a fatal exception, or if the retriable attempts were exhausted.
     *
     * @param executionContext the execution context, carrying execution state across actions
     * @throws RetryableJobExecutionException in case of error that deserves another attempt (for example, temporary
     *                                        failure to connect to the server
     * @throws FatalJobExecutionException     in case of error that should not be retried (for example, trying to use a
     *                                        job id that was already in use)
     */
    protected abstract void attemptStageAction(
        ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException;
}
