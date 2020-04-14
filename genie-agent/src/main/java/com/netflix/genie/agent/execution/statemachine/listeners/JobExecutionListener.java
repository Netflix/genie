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

import javax.annotation.Nullable;

/**
 * Listener of job execution.
 * Notifications are delivered synchronously from the state machine execution thread.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface JobExecutionListener {

    /**
     * Invoked when the state machine reaches the next state, before any of the logic for that state is evaluated.
     *
     * @param state the state entered
     */
    default void stateEntered(States state) {
    }

    /**
     * Invoked when the state machine is done processing a state and it is about to enter the next one.
     *
     * @param state the state exiting
     */
    default void stateExited(States state) {
    }

    /**
     * Invoked before the action associated to a given state is invoked.
     *
     * @param state the state whose action is invoked
     */
    default void beforeStateActionAttempt(States state) {
    }

    /**
     * Invoked after the action associated with a given state is invoked.
     *
     * @param state     the state whose action was invoked
     * @param exception the exception thrown by the action, or null if there was no error
     */
    default void afterStateActionAttempt(States state, @Nullable Exception exception) {
    }

    /**
     * Invoked when the state machine starts running.
     */
    default void stateMachineStarted() {
    }

    /**
     * Invoked when the state machine stops.
     */
    default void stateMachineStopped() {
    }

    /**
     * Invoked when the action of a given state is skipped because execution was aborted and the state is set to be
     * skipped in case of aborted execution.
     *
     * @param state the state whose action is skipped
     */
    default void stateSkipped(States state) {
    }

    /**
     * Invoked when the state machine encounters a fatal exception while invoking the action for a given state.
     *
     * @param state     the state whose action produced a fatal exception
     * @param exception the exception
     */
    default void fatalException(States state, FatalJobExecutionException exception) {
    }

    /**
     * Invoked when the state machine encounters a fatal exception in a critical state, which causes the execution to
     * abort.
     *
     * @param state     the state whose action produced a fatal exception
     * @param exception the exception
     */
    default void executionAborted(States state, FatalJobExecutionException exception) {
    }

    /**
     * Invoked when the state machine is about to wait before retrying the action associated to the given state as a
     * result of a retryable exception on the previous attempt.
     *
     * @param state      the state whose action will be retried after a pause
     * @param retryDelay the retry delay in milliseconds
     */
    default void delayedStateActionRetry(States state, final long retryDelay) {
    }
}
