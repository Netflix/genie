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

import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.Message;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.action.Action;
import org.springframework.statemachine.state.State;
import org.springframework.statemachine.transition.Transition;
import org.springframework.statemachine.trigger.Trigger;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;

/**
 * Listener that logs state machine events and transitions.
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
public class LoggingListener implements JobExecutionListener {

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateChanged(@Nullable final State<States, Events> from, final State<States, Events> to) {
        log.info(
            "State changed: {} -> {}",
            getStateNameString(from),
            getStateNameString(to)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateEntered(final State<States, Events> state) {
        log.info(
            "Entered state: {} ({}/{}/{} entry/state/exit actions)",
            getStateNameString(state),
            state.getEntryActions() == null ? 0 : state.getEntryActions().size(),
            state.getStateActions() == null ? 0 : state.getStateActions().size(),
            state.getExitActions() == null ? 0 : state.getExitActions().size()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateExited(final State<States, Events> state) {
        log.info(
            "Exited state: {}",
            getStateNameString(state)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void eventNotAccepted(final Message<Events> event) {
        log.info(
            "Event not accepted: {}",
            event.getPayload()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transition(final Transition<States, Events> transition) {
        log.debug(
            "Triggered {} transition from {} to {} ({} actions)",
            transition.getKind(),
            getStateNameString(transition.getSource()),
            getStateNameString(transition.getTarget()),
            transition.getActions() != null ? transition.getActions().size() : "0"
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionStarted(final Transition<States, Events> transition) {
        log.info(
            "Starting {} transition: {} -> {} ({} actions) ({})",
            transition.getKind().toString(),
            getStateNameString(transition.getSource()),
            getStateNameString(transition.getTarget()),
            transition.getActions() != null ? transition.getActions().size() : 0,
            getTriggerString(transition)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionEnded(final Transition<States, Events> transition) {
        log.info(
            "Ended {} transition: {} -> {}",
            transition.getKind().toString(),
            getStateNameString(transition.getSource()),
            getStateNameString(transition.getTarget())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateMachineStarted(final StateMachine<States, Events> stateMachine) {
        log.info(
            "State machine started"
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateMachineStopped(final StateMachine<States, Events> stateMachine) {
        log.info(
            "State machine stopped in state: {}",
            getStateNameString(stateMachine.getState())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateMachineError(final StateMachine<States, Events> stateMachine, final Exception exception) {
        log.info(
            "State machine error: {} - {}",
            exception.getClass().getSimpleName(),
            exception.getMessage()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void extendedStateChanged(final Object key, final Object value) {
        log.info(
            "Extended state change: '{}' = '{}'",
            key,
            value
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateContext(final StateContext<States, Events> stateContext) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecute(
        final StateMachine<States, Events> stateMachine,
        final Action<States, Events> action,
        final long duration
    ) {
        log.info(
            "Executed action: {} in {} ms (current state: {})",
            action.getClass().getSimpleName(),
            duration,
            getStateNameString(stateMachine.getState())
        );
    }


    private String getStateNameString(@Nullable final State<States, Events> state) {
        return state != null ? state.getId().toString() : "(null)";
    }

    private String getTriggerString(final Transition<States, Events> transition) {

        final StringBuilder stringBuilder = new StringBuilder();

        final Trigger<States, Events> trigger = transition.getTrigger();

        stringBuilder.append("trigger: ");

        if (trigger == null) {
            stringBuilder.append("null");
        } else {
            stringBuilder.append(trigger.getClass().getSimpleName());
            final Events event = trigger.getEvent();
            if (event != null) {
                stringBuilder
                    .append(" event: ")
                    .append(event.toString());
            }
        }

        return stringBuilder.toString();
    }
}
