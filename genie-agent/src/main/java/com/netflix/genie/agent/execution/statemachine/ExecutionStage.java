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
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.action.Action;

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
    static final String EXECUTION_CONTEXT_CONTEXT_KEY = "genie_job_execution_context";
    private final States state;
    private final Action<States, Events> stateAction;
    private final Action<States, Events> transitionAction;
    private final Action<States, Events> transitionErrorAction;

    /**
     * Constructor.
     *
     * @param state the state machine state associated with this stage
     */
    protected ExecutionStage(
        final States state
    ) {
        this.state = state;

        // Action performed when entering a state for the first time
        this.stateAction = this::executeStateAction;

        // Action performed while transitioning from one state to the next
        this.transitionAction = this::executeTransitionAction;

        // Action performed when transition action throws an exception
        this.transitionErrorAction = this::handleTransitionActionError;
    }

    private void executeStateAction(final StateContext<States, Events> context) {
        // Reset the number of attempts for the next transition
        final ExecutionContext executionContext = getExecutionContext(context);
        executionContext.setAttemptsLeft(1 + state.getTransitionRetries());
        // Trigger the transition to next state
        context.getStateMachine().sendEvent(Events.PROCEED);
    }

    private void executeTransitionAction(final StateContext<States, Events> context) {
        final ExecutionContext executionContext = getExecutionContext(context);

        final boolean executionAborted = executionContext.isExecutionAborted();
        final String stateName = this.state.name();
        final int attemptsLeft = executionContext.getAttemptsLeft();

        if (executionAborted && this.state.isSkippedDuringAbortedExecution()) {
            log.info("Skipping transition action of state {} due to execution aborted", stateName);
        } else if (attemptsLeft > 0) {
            // Decrement attempts left
            executionContext.setAttemptsLeft(attemptsLeft - 1);
            log.info("Attempting transition action of state {}", stateName);
            this.attemptTransition(executionContext);
        } else {
            // Out of retries (or fatal error which set attempts left to 0)
            log.info("Skipping transition action of state {} due to errors", stateName);
        }
    }


    private void handleTransitionActionError(final StateContext<States, Events> context) {
        final String stateName = this.state.name();
        final Exception transitionException = context.getException();
        final ExecutionContext executionContext = getExecutionContext(context);

        log.warn(
            "Transition action of state {} failed with error: {}: {}",
            stateName,
            transitionException.getClass().getSimpleName(),
            transitionException.getMessage()
        );

        final int attemptsLeft = executionContext.getAttemptsLeft();

        final Exception recordedException;

        if (transitionException instanceof FatalTransitionException) {
            // Record fatal exception (which aborts execution)
            recordedException = transitionException;
            // Zero attempts left, fatal error means retries are unnecessary
            executionContext.setAttemptsLeft(0);
        } else if (transitionException instanceof RetryableTransitionException && attemptsLeft > 0) {
            // Record retryable exception
            // TODO delay retry in this case
            recordedException = transitionException;
        } else if (transitionException instanceof RetryableTransitionException && attemptsLeft == 0) {
            // Out of retries, make a fatal exception out of the retryable
            recordedException = this.createFatalException(
                "No more attempts left for retryable error in state " + this.state.name(),
                transitionException
            );
        } else {
            // Unhandled exception, consider it fatal
            recordedException = this.createFatalException(
                "Unhandled transition exception in state " + this.state.name() + ": " + transitionException,
                transitionException
            );
            // Zero attempts left, this should never happen
            executionContext.setAttemptsLeft(0);
        }

        // Save error in history
        executionContext.recordTransitionException(this.state, recordedException);

        if (
            this.state.isCriticalState()
                && recordedException instanceof FatalTransitionException
                && executionContext.getExecutionAbortedFatalException() == null
        ) {
            log.warn("Aborting execution due to fatal error in state: {}", this.state.name());
            executionContext.setExecutionAbortedFatalException((FatalTransitionException) recordedException);
        }

        // Re-trigger transition action (may be skipped this time, depending on the error)
        context.getStateMachine().sendEvent(Events.PROCEED);
    }

    protected FatalTransitionException createFatalException(final String message, final Throwable cause) {
        return new FatalTransitionException(this.getState(), message, cause);
    }

    protected FatalTransitionException createFatalException(final Throwable cause) {
        return this.createFatalException(
            "Fatal error in state " + this.getState().name() + ": " + cause.getMessage(),
            cause
        );
    }

    protected RetryableTransitionException createRetryableException(final Throwable cause) {
        throw new RetryableTransitionException(
            "Retryable error in state " + this.getState().name() + ": " + cause.getMessage(),
            cause
        );
    }

    private ExecutionContext getExecutionContext(final StateContext<States, Events> context) {
        return context.getExtendedState().get(EXECUTION_CONTEXT_CONTEXT_KEY, ExecutionContext.class);
    }

    /**
     * Action associated with this stage. May be skipped if execution is aborted and the stage is defined as skippable,
     * or if the action was attempted and threw a fatal exception, or if the retriable attempts were exhausted.
     *
     * @param executionContext the execution context, carrying execution state across actions
     * @throws RetryableTransitionException in case of error that deserves another attempt (for example, temporary
     *                                      failure to connect to the server
     * @throws FatalTransitionException     in case of error that should not be retried and should cause the execution
     *                                      to be aborted (for example, trying to use a job id that was already in use)
     */
    protected abstract void attemptTransition(
        ExecutionContext executionContext
    ) throws RetryableTransitionException, FatalTransitionException;
}
