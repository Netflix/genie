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
import com.netflix.genie.agent.execution.statemachine.listeners.LoggingListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
 * Spring auto configuration of {@link JobExecutionStateMachine} state machine for the agent process.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
@Slf4j
public class StateMachineAutoConfiguration {

    private static final String STATE_MACHINE_ID = "job-execution";

    /**
     * Provide a lazy {@link LoggingListener} bean if one hasn't already been defined.
     *
     * @return A {@link LoggingListener} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(LoggingListener.class)
    public LoggingListener loggingListener() {
        return new LoggingListener();
    }

    /**
     * Provide a lazy {@link JobExecutionStateMachine} bean if one hasn't already been defined.
     *
     * @param stateMachine The state machine to use for job execution
     * @return A {@link JobExecutionStateMachineImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(JobExecutionStateMachine.class)
    public JobExecutionStateMachine jobExecutionStateMachine(final StateMachine<States, Events> stateMachine) {
        return new JobExecutionStateMachineImpl(stateMachine);
    }

    /**
     * Provide a lazy {@link StateMachine} instance configured with the current model expected for job execution. It is
     * not recommended to override this bean but it is possible.
     *
     * @param statesWithActions         The states to use that have actions associated with them available in the
     *                                  Spring context
     * @param eventDrivenTransitions    The event driven transitions available in the Spring context
     * @param statesWithErrorTransition The states that have error transitions associated with them available in the
     *                                  Spring context
     * @param listeners                 Any {@link JobExecutionListener} implementations available in the Spring context
     * @return A {@link StateMachine} instance configured with the available options from the application context
     * @throws Exception On any error configuring the state machine
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(StateMachine.class)
    public StateMachine<States, Events> stateMachine(
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

    /**
     * Provide a lazy bean which is a collection of all the states the state machine should contain which have an
     * associated action. All should implement {@link StateAction}.
     *
     * @param initializeAction              The initialization action
     * @param configureAgentAction          The configure agent action
     * @param resolveJobSpecificationAction The resolve job specification action
     * @param setUpJobAction                The setup job action
     * @param launchJobAction               The launch job action
     * @param monitorJobAction              The monitor job action
     * @param cleanupJobAction              The cleanup job action
     * @param shutdownAction                The shut down action
     * @param handleErrorAction             The handle error action
     * @return A collection of all the actions
     */
    @Bean
    @Lazy
    public Collection<Pair<States, StateAction>> statesWithActions(
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

    /**
     * Provide a lazy bean definition for the event driven transitions within the state machine.
     *
     * @return A collection of transitions based on source state, event, destination state
     */
    @Bean
    @Lazy
    public Collection<Triple<States, Events, States>> eventDrivenTransitions() {
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

    /**
     * Provide a bean with a collection of states which should have transitions to an error action when an error occurs.
     *
     * @return The collection of {@link States}
     */
    @Bean
    @Lazy
    public Collection<States> statesWithErrorTransition() {
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
