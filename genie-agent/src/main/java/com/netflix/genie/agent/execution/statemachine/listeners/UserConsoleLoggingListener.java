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
package com.netflix.genie.agent.execution.statemachine.listeners;

import com.netflix.genie.agent.cli.UserConsole;
import com.netflix.genie.agent.execution.statemachine.FatalTransitionException;
import com.netflix.genie.agent.execution.statemachine.States;
import org.slf4j.Logger;

import javax.annotation.Nullable;

/**
 * Job execution listener that prints messages visible to the user in the console.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class UserConsoleLoggingListener implements JobExecutionListener {
    private final Logger log;

    /**
     * Constructor.
     */
    public UserConsoleLoggingListener() {
        this.log = UserConsole.getLogger();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateEntered(final States state) {
        log.debug("Entered state: {}", state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateExited(final States state) {
        log.debug("Exiting state: {}", state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void beforeStateActionAttempt(final States state) {
        log.info(" > {} <", state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void afterStateActionAttempt(final States state, @Nullable final Exception exception) {
        if (exception != null) {
            log.warn("{} error: {}", state, exception.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateMachineStarted() {
        log.info("Starting job execution...");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateMachineStopped() {
        log.info("Job execution completed");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateSkipped(final States state) {
        log.debug(" > Skip: {}", state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fatalException(final States state, final FatalTransitionException exception) {
        log.debug("{}: {}", state, exception.getMessage());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void executionAborted(final States state, final FatalTransitionException exception) {
        log.warn("Job execution aborted in {}: {}", state, exception.getMessage());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delayedStateActionRetry(final States state, final long retryDelay) {
        log.debug("Retryable error in {}, next attempt in {}ms", state, retryDelay);
    }
}
