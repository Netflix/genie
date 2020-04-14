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
package com.netflix.genie.agent.execution.statemachine.listeners;

import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

/**
 * Listener that logs state machine events and transitions.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class LoggingListener implements JobExecutionListener {

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateEntered(final States state) {
        log.info("Entering state {}", state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateExited(final States state) {
        log.info("Exiting state {}", state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void beforeStateActionAttempt(final States state) {
        log.debug("About to execute state {} action", state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void afterStateActionAttempt(final States state, @Nullable final Exception exception) {
        if (exception == null) {
            log.debug("State {} action", state);
        } else {
            log.warn("State {} action threw {}: {}", state, exception.getClass(), exception.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateMachineStarted() {
        log.info("State machine started");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateMachineStopped() {
        log.info("State machine stopped");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateSkipped(final States state) {
        log.warn("State {} action skipped due to aborted execution", state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fatalException(final States state, final FatalJobExecutionException exception) {
        log.error("Fatal exception in state {}: {}", state, exception.getMessage(), exception);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void executionAborted(final States state, final FatalJobExecutionException exception) {
        log.info("Execution aborted in state {} due to: {}", state, exception.getMessage());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delayedStateActionRetry(final States state, final long retryDelay) {
        log.debug("Retry for state {} action in {}ms", state, retryDelay);
    }
}
