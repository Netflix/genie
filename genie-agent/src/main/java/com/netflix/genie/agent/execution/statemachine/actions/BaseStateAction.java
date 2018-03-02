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

package com.netflix.genie.agent.execution.statemachine.actions;

import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.StateContext;

/**
 * Base class for StateAction.
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public abstract class BaseStateAction implements StateAction {

    private final ExecutionContext executionContext;

    protected BaseStateAction(final ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void execute(final StateContext<States, Events> context) {

        final States currentState = context.getStateMachine().getState().getId();
        final String currentActionName = this.getClass().getSimpleName();

        Events nextEvent;
        try {
            log.info("Performing state {} action: {}", currentState, currentActionName);
            // State action returns the next event to send (or null)
            nextEvent = executeStateAction(executionContext);
            log.info("State action {} returned {} as next event", currentActionName, nextEvent);
        } catch (final Exception e) {
            nextEvent = Events.ERROR;
            if (!context.getStateMachine().hasStateMachineError()) {
                // Set error, will enable transitions to HANDLE_ERROR state.
                // Don't override if an earlier one is set, original one is more relevant.
                context.getStateMachine().setStateMachineError(e);
            }
            log.error(
                "Action {} failed with exception",
                this.getClass().getSimpleName(),
                e
            );
        }

        final boolean accepted = context.getStateMachine().sendEvent(nextEvent);
        if (!accepted) {
            log.warn(
                "State machine in state {} rejected event {}",
                currentState,
                nextEvent
            );
        }
    }

    protected abstract Events executeStateAction(final ExecutionContext ctx);
}
