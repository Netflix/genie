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
import org.springframework.messaging.Message;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.action.Action;
import org.springframework.statemachine.state.State;
import org.springframework.statemachine.transition.Transition;

/**
 * Adapter for implementations of JobExecutionListener.
 * Avoid the need to override all methods with empty implementations.
 *
 * @author mprimi
 * @since 4.0.0
 */
class JobExecutionListenerAdapter implements JobExecutionListener {

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecute(
        final StateMachine<States, Events> stateMachine,
        final Action<States, Events> action,
        final long duration
    ) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateChanged(final State<States, Events> from, final State<States, Events> to) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateEntered(final State<States, Events> state) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateExited(final State<States, Events> state) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void eventNotAccepted(final Message<Events> event) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transition(final Transition<States, Events> transition) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionStarted(final Transition<States, Events> transition) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionEnded(final Transition<States, Events> transition) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateMachineStarted(final StateMachine<States, Events> stateMachine) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateMachineStopped(final StateMachine<States, Events> stateMachine) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateMachineError(final StateMachine<States, Events> stateMachine, final Exception exception) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void extendedStateChanged(final Object key, final Object value) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateContext(final StateContext<States, Events> stateContext) { }
}
