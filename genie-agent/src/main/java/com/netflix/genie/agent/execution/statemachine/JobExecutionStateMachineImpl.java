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

import com.google.common.collect.ImmutableSet;
import com.netflix.genie.agent.execution.process.JobProcessManager;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.statemachine.listeners.JobExecutionListener;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of the job execution state machine.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class JobExecutionStateMachineImpl implements JobExecutionStateMachine {
    private static final long RETRY_DELAY = 250;
    @Getter
    private final List<ExecutionStage> executionStages;
    @Getter
    private final ExecutionContext executionContext;
    private final JobExecutionListener listener;
    private final JobProcessManager jobProcessManager;

    /**
     * Constructor.
     *
     * @param executionStages   the (ordered) list of execution stages
     * @param executionContext  the execution context passed across stages during execution
     * @param listeners         the list of listeners
     * @param jobProcessManager the job process manager
     */
    public JobExecutionStateMachineImpl(
        final List<ExecutionStage> executionStages,
        final ExecutionContext executionContext,
        final Collection<JobExecutionListener> listeners,
        final JobProcessManager jobProcessManager
    ) {
        this.executionStages = executionStages;
        this.executionContext = executionContext;
        this.listener = new CompositeListener(ImmutableSet.copyOf(listeners));
        this.jobProcessManager = jobProcessManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        if (!this.executionContext.getStarted().compareAndSet(false, true)) {
            throw new IllegalStateException("Called run on an already started state machine");
        }

        this.executionContext.setStateMachine(this);

        this.listener.stateMachineStarted();

        for (final ExecutionStage executionStage : this.executionStages) {
            final States state = executionStage.getState();

            log.debug("Execution stage: {} for state {} ({}, {}, {} retries)",
                executionStage.getClass().getSimpleName(),
                state.name(),
                state.isCriticalState() ? "CRITICAL" : "NON-CRITICAL",
                state.isSkippedDuringAbortedExecution() ? "SKIP" : "NON-SKIP",
                state.getTransitionRetries()
            );

            this.listener.stateEntered(state);

            this.executeStageAction(state, executionStage);

            this.listener.stateExited(state);
        }

        this.listener.stateEntered(States.DONE);
        this.listener.stateMachineStopped();
    }

    @Override
    public void kill(final KillService.KillSource killSource) {
        log.info("Shutting down job execution (kill event source: {}", killSource);
        if (killSource == KillService.KillSource.REMOTE_STATUS_MONITOR) {
            this.executionContext.setSkipFinalStatusUpdate(true);
        }
        this.executionContext.setJobKilled(true);
        this.jobProcessManager.kill(killSource);
    }

    private void executeStageAction(final States state, final ExecutionStage executionStage) {

        // Reset retries backoff
        long currentRetryDelay = 0;

        int retriesLeft = state.getTransitionRetries();
        while (true) {

            // If execution is a aborted and this is a skip state, stop.
            if (this.executionContext.isExecutionAborted() && state.isSkippedDuringAbortedExecution()) {
                this.listener.stateSkipped(state);
                log.debug("Skipping stage {} due to aborted execution", state);
                return;
            }

            // Attempt the stage action
            this.listener.beforeStateActionAttempt(state);
            Exception exception = null;
            try {
                executionStage.attemptStageAction(executionContext);
            } catch (Exception e) {
                log.debug("Exception in state: " + state, e);
                exception = e;
            }
            this.listener.afterStateActionAttempt(state, exception);

            // No exception, stop
            if (exception == null) {
                log.debug("Stage execution successful");
                return;
            }

            // Record the raw exception
            this.executionContext.recordTransitionException(state, exception);

            FatalJobExecutionException fatalJobExecutionException = null;

            if (exception instanceof RetryableJobExecutionException && retriesLeft > 0) {
                // Try action again after a delay
                retriesLeft--;
                currentRetryDelay += RETRY_DELAY;

            } else if (exception instanceof RetryableJobExecutionException) {
                // No retries left, save as fatal exception
                fatalJobExecutionException = new FatalJobExecutionException(
                    state,
                    "No more attempts left for retryable error in state " + state,
                    exception
                );
                this.executionContext.recordTransitionException(state, fatalJobExecutionException);

            } else if (exception instanceof FatalJobExecutionException) {
                // Save fatal exception
                fatalJobExecutionException = (FatalJobExecutionException) exception;
            } else {
                // Create fatal exception out of unexpected exception
                fatalJobExecutionException = new FatalJobExecutionException(
                    state,
                    "Unhandled exception" + exception.getMessage(),
                    exception
                );
                this.executionContext.recordTransitionException(state, fatalJobExecutionException);
            }

            if (fatalJobExecutionException != null) {

                if (state.isCriticalState() && !executionContext.isExecutionAborted()) {
                    // Fatal exception in critical stage aborts execution, unless it's already aborted
                    this.executionContext.setExecutionAbortedFatalException(fatalJobExecutionException);
                    this.listener.executionAborted(state, fatalJobExecutionException);
                }

                this.listener.fatalException(state, fatalJobExecutionException);
                // Fatal exception always stops further attempts
                return;
            }

            // Calculate delay before next retry
            // Delay the next attempt
            log.debug("Action will be attempted again in {}ms", currentRetryDelay);
            this.listener.delayedStateActionRetry(state, currentRetryDelay);
            try {
                Thread.sleep(currentRetryDelay);
            } catch (InterruptedException ex) {
                log.info("Interrupted during delayed retry");
            }
        }
    }

    private static final class CompositeListener implements JobExecutionListener {
        private final Collection<JobExecutionListener> listeners;

        private CompositeListener(final Collection<JobExecutionListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void stateEntered(final States state) {
            listeners.forEach(listener -> listener.stateEntered(state));
        }

        @Override
        public void stateExited(final States state) {
            listeners.forEach(listener -> listener.stateExited(state));
        }

        @Override
        public void beforeStateActionAttempt(final States state) {
            listeners.forEach(listener -> listener.beforeStateActionAttempt(state));
        }

        @Override
        public void afterStateActionAttempt(final States state, @Nullable final Exception exception) {
            listeners.forEach(listener -> listener.afterStateActionAttempt(state, exception));
        }

        @Override
        public void stateMachineStarted() {
            listeners.forEach(JobExecutionListener::stateMachineStarted);
        }

        @Override
        public void stateMachineStopped() {
            listeners.forEach(JobExecutionListener::stateMachineStopped);
        }

        @Override
        public void stateSkipped(final States state) {
            listeners.forEach(listener -> listener.stateSkipped(state));
        }

        @Override
        public void fatalException(final States state, final FatalJobExecutionException exception) {
            listeners.forEach(listener -> listener.fatalException(state, exception));
        }

        @Override
        public void executionAborted(final States state, final FatalJobExecutionException exception) {
            listeners.forEach(listener -> listener.executionAborted(state, exception));
        }

        @Override
        public void delayedStateActionRetry(final States state, final long retryDelay) {
            listeners.forEach(listener -> listener.delayedStateActionRetry(state, retryDelay));
        }
    }
}
