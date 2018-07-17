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

package com.netflix.genie.agent.execution.statemachine;

import com.netflix.genie.agent.execution.statemachine.actions.StateAction;
import com.netflix.genie.agent.execution.statemachine.listeners.JobExecutionListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.config.StateMachineBuilder;
import org.springframework.statemachine.config.builders.StateMachineTransitionConfigurer;
import org.springframework.statemachine.config.configurers.StateConfigurer;
import org.springframework.statemachine.state.State;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

/**
 * Configuration of JobExecutionStateMachine state machine.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
@Slf4j
class StateMachineConfig {

    private static final String STATE_MACHINE_ID = "job-execution";

    @Bean
    @Lazy
    StateMachine<States, Events> stateMachine(
        final Collection<Pair<States, StateAction>> statesWithActions,
        final Collection<Triple<States, Events, States>> eventDrivenTransitions,
        final Collection<States> statesWithErrorTransition,
        final Collection<JobExecutionListener> listeners
    ) throws Exception {

        final StateMachineBuilder.Builder<States, Events> builder = new StateMachineBuilder.Builder<>();

        configureConfiguration(builder);

        configureStates(builder, statesWithActions);

        configureTransitions(builder, eventDrivenTransitions, statesWithErrorTransition);

        // Build state machine
        final StateMachine<States, Events> stateMachine = builder.build();

        registerListeners(listeners, stateMachine);

        return stateMachine;
    }

    @Bean
    @Lazy
    Collection<Pair<States, StateAction>> statesWithActions(
        final StateAction.Initialize initializeAction,
        final StateAction.ConfigureAgent configureAgentAction,
        final StateAction.ResolveJobSpecification resolveJobSpecificationAction,
        final StateAction.SetUpJob setUpJobAction,
        final StateAction.LaunchJob launchJobAction,
        final StateAction.MonitorJob monitorJobAction,
        final StateAction.CleanupJob cleanupJobAction,
        final StateAction.Shutdown shutdownAction,
        final StateAction.HandleError handleErrorAction
    ) {
        return Arrays.asList(
            Pair.of(States.INITIALIZE, initializeAction),
            Pair.of(States.CONFIGURE_AGENT, configureAgentAction),
            Pair.of(States.RESOLVE_JOB_SPECIFICATION, resolveJobSpecificationAction),
            Pair.of(States.SETUP_JOB, setUpJobAction),
            Pair.of(States.LAUNCH_JOB, launchJobAction),
            Pair.of(States.MONITOR_JOB, monitorJobAction),
            Pair.of(States.CLEANUP_JOB, cleanupJobAction),
            Pair.of(States.SHUTDOWN, shutdownAction),
            Pair.of(States.HANDLE_ERROR, handleErrorAction)
        );
    }

    @Bean
    @Lazy
    Collection<Triple<States, Events, States>> eventDrivenTransitions() {
        return Arrays.asList(
            // Regular execution
            Triple.of(States.READY, Events.START, States.INITIALIZE),
            Triple.of(States.INITIALIZE, Events.INITIALIZE_COMPLETE, States.CONFIGURE_AGENT),
            Triple.of(States.CONFIGURE_AGENT, Events.CONFIGURE_AGENT_COMPLETE, States.RESOLVE_JOB_SPECIFICATION),
            Triple.of(States.RESOLVE_JOB_SPECIFICATION, Events.RESOLVE_JOB_SPECIFICATION_COMPLETE, States.SETUP_JOB),
            Triple.of(States.SETUP_JOB, Events.SETUP_JOB_COMPLETE, States.LAUNCH_JOB),
            Triple.of(States.LAUNCH_JOB, Events.LAUNCH_JOB_COMPLETE, States.MONITOR_JOB),
            Triple.of(States.MONITOR_JOB, Events.MONITOR_JOB_COMPLETE, States.CLEANUP_JOB),
            Triple.of(States.CLEANUP_JOB, Events.CLEANUP_JOB_COMPLETE, States.SHUTDOWN),
            Triple.of(States.SHUTDOWN, Events.SHUTDOWN_COMPLETE, States.END),
            // Job cancellation
            Triple.of(States.READY, Events.CANCEL_JOB_LAUNCH, States.CLEANUP_JOB),
            Triple.of(States.INITIALIZE, Events.CANCEL_JOB_LAUNCH, States.CLEANUP_JOB),
            Triple.of(States.CONFIGURE_AGENT, Events.CANCEL_JOB_LAUNCH, States.CLEANUP_JOB),
            Triple.of(States.RESOLVE_JOB_SPECIFICATION, Events.CANCEL_JOB_LAUNCH, States.CLEANUP_JOB),
            Triple.of(States.SETUP_JOB, Events.CANCEL_JOB_LAUNCH, States.CLEANUP_JOB)
        );
    }

    @Bean
    @Lazy
    Collection<States> statesWithErrorTransition() {
        return EnumSet.of(
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.SETUP_JOB,
            States.LAUNCH_JOB,
            States.MONITOR_JOB,
            States.CLEANUP_JOB,
            States.SHUTDOWN
        );
    }

    private void configureConfiguration(
        final StateMachineBuilder.Builder<States, Events> builder
    ) throws Exception {
        builder.configureConfiguration()
            .withConfiguration()
            .machineId(STATE_MACHINE_ID);
    }

    private void configureStates(
        final StateMachineBuilder.Builder<States, Events> builder,
        final Collection<Pair<States, StateAction>> statesWithActions
    ) throws Exception {
        // Set up initial and terminal states (action-free)
        final StateConfigurer<States, Events> stateConfigurer = builder.configureStates()
            .withStates()
            .initial(States.READY)
            .end(States.END);

        // Set up the rest of the states with their corresponding action
        for (Pair<States, StateAction> stateWithAction : statesWithActions) {
            final States state = stateWithAction.getLeft();
            final StateAction action = stateWithAction.getRight();
            stateConfigurer
                // Use entryAction because it is not interruptible.
                // StateAction is susceptible to cancellation in case of event-triggered transition out of the state.
                .state(state, action, null);
            log.info(
                "Configured state {} with action {}",
                state,
                action.getClass().getSimpleName()
            );
        }
    }

    private void configureTransitions(
        final StateMachineBuilder.Builder<States, Events> builder,
        final Collection<Triple<States, Events, States>> eventDrivenTransitions,
        final Collection<States> statesWithErrorTransition
    ) throws Exception {
        final StateMachineTransitionConfigurer<States, Events> transitionConfigurer = builder.configureTransitions();

        // Set up event-driven transitions
        for (Triple<States, Events, States> transition : eventDrivenTransitions) {
            final States sourceState = transition.getLeft();
            final States targetState = transition.getRight();
            final Events event = transition.getMiddle();
            transitionConfigurer
                .withExternal()
                .source(sourceState)
                .target(targetState)
                .event(event)
                .and();
            log.info(
                "Configured event-driven transition: ({}) -> [{}] -> ({})",
                sourceState,
                event,
                targetState
            );
        }

        // Set up transitions to HANDLE_ERROR state.
        for (States state : statesWithErrorTransition) {
            transitionConfigurer
                .withExternal()
                .source(state)
                .target(States.HANDLE_ERROR)
                .event(Events.ERROR)
                .and();
            log.info(
                "Configured error transition: ({}) -> ({})",
                state,
                States.HANDLE_ERROR
            );
        }

        // Add transition from HANDLE_ERROR to END
        transitionConfigurer
            .withExternal()
            .source(States.HANDLE_ERROR)
            .target(States.END)
            .event(Events.HANDLE_ERROR_COMPLETE);
    }

    private void registerListeners(
        final Collection<JobExecutionListener> listeners,
        final StateMachine<States, Events> stateMachine
    ) {
        // Add state machine listeners
        listeners.forEach(stateMachine::addStateListener);

        // Add state action listeners to each state
        for (JobExecutionListener listener : listeners) {
            stateMachine.addStateListener(listener);
            for (State<States, Events> state : stateMachine.getStates()) {
                state.addActionListener(listener);
            }
        }
    }
}
