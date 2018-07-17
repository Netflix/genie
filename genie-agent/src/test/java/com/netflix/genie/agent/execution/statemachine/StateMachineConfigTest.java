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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.agent.execution.statemachine.actions.StateAction;
import com.netflix.genie.agent.execution.statemachine.listeners.JobExecutionListener;
import com.netflix.genie.agent.execution.statemachine.listeners.LoggingListener;
import com.netflix.genie.test.categories.UnitTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.messaging.Message;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.action.Action;
import org.springframework.statemachine.listener.StateMachineListenerAdapter;
import org.springframework.statemachine.state.State;
import org.springframework.statemachine.support.StateMachineInterceptorAdapter;
import org.springframework.statemachine.test.StateMachineTestPlanBuilder;
import org.springframework.statemachine.transition.Transition;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test the state machine configured for job execution using mock state actions.
 */
@Slf4j
@Category(UnitTest.class)
public class StateMachineConfigTest {

    private static final int MIN_ACTION_DURATION_MS = 10;
    private static final int MAX_ACTION_DURATION_MS = 300;
    private static final int AWAIT_TIME = 5;

    private List<States> visitedStates;
    private List<Action<States, Events>> executedActions;
    private StateMachine<States, Events> sm;
    private Collection<Pair<States, StateAction>> statesWithActions;
    private StateAction.Initialize initializeActionMock;
    private StateAction.ConfigureAgent configureAgentActionMock;
    private StateAction.ResolveJobSpecification resolveJobSpecificationActionMock;
    private StateAction.SetUpJob setupJobActionMock;
    private StateAction.LaunchJob launchJobActionMock;
    private StateAction.MonitorJob monitorJobActionMock;
    private StateAction.CleanupJob cleanupJobActionMock;
    private StateAction.Shutdown shutdownActionMock;
    private StateAction.HandleError handleErrorActionMock;

    /**
     * Set up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception {
        this.visitedStates = Lists.newArrayList();
        this.executedActions = Lists.newArrayList();

        this.initializeActionMock =
            new TestStateAction(States.INITIALIZE, Events.INITIALIZE_COMPLETE);
        this.configureAgentActionMock =
            new TestStateAction(States.CONFIGURE_AGENT, Events.CONFIGURE_AGENT_COMPLETE);
        this.resolveJobSpecificationActionMock =
            new TestStateAction(States.RESOLVE_JOB_SPECIFICATION, Events.RESOLVE_JOB_SPECIFICATION_COMPLETE);
        this.setupJobActionMock =
            new TestStateAction(States.SETUP_JOB, Events.SETUP_JOB_COMPLETE);
        this.launchJobActionMock =
            new TestStateAction(States.LAUNCH_JOB, Events.LAUNCH_JOB_COMPLETE);
        this.monitorJobActionMock =
            new TestStateAction(States.MONITOR_JOB, Events.MONITOR_JOB_COMPLETE);
        this.cleanupJobActionMock =
            new TestStateAction(States.CLEANUP_JOB, Events.CLEANUP_JOB_COMPLETE);
        this.shutdownActionMock =
            new TestStateAction(States.SHUTDOWN, Events.SHUTDOWN_COMPLETE);
        this.handleErrorActionMock =
            new TestStateAction(States.HANDLE_ERROR, Events.HANDLE_ERROR_COMPLETE);

        final StateMachineConfig config = new StateMachineConfig();

        this.statesWithActions = config.statesWithActions(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            setupJobActionMock,
            launchJobActionMock,
            monitorJobActionMock,
            cleanupJobActionMock,
            shutdownActionMock,
            handleErrorActionMock
        );

        this.sm = config.stateMachine(
            statesWithActions,
            config.eventDrivenTransitions(),
            config.statesWithErrorTransition(),
            Lists.newArrayList(new LoggingListener(), new ExecutionPathListener())
        );
    }

    /**
     * Execution with no errors or cancellation.
     *
     * @throws Exception the exception
     */
    @Test
    public void successfulExecutionTest() throws Exception {
        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectState(States.END)
            .expectStateChanged(9)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.SETUP_JOB,
            States.LAUNCH_JOB,
            States.MONITOR_JOB,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            setupJobActionMock,
            launchJobActionMock,
            monitorJobActionMock,
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Cancel event before job execution is started.
     *
     * @throws Exception the exception
     */
    @Test
    public void cancelBeforeStartTest() throws Exception {
        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.CANCEL_JOB_LAUNCH)
            .expectState(States.END)
            .expectStateChanged(3)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Handle cancel signal as state machine is entering CONFIGURE_AGENT state.
     *
     * @throws Exception the exception
     */
    @Test
    public void cancelBeforeAgentConfigure() throws Exception {

        cancelJobBeforeEnteringState(States.CONFIGURE_AGENT);

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectState(States.END)
            .expectStateChanged(5)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Handle cancel signal as state machine is preparing to exit CONFIGURE_AGENT state.
     *
     * @throws Exception the exception
     */
    @Test
    public void cancelAfterAgentConfigure() throws Exception {
        cancelJobAfterEnteringState(States.CONFIGURE_AGENT);

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectState(States.END)
            .expectStateChanged(6)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Handle cancel signal as state machine is executing CONFIGURE_AGENT state action.
     *
     * @throws Exception the exception
     */
    @Test
    public void cancelDuringAgentConfigure() throws Exception {

        ((TestStateAction) configureAgentActionMock).cancelJobDuringExecution();

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectState(States.END)
            .expectStateChanged(5)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Handle cancel signal as state machine is entering SETUP_JOB state.
     *
     * @throws Exception the exception
     */
    @Test
    public void cancelBeforeSetup() throws Exception {
        cancelJobBeforeEnteringState(States.SETUP_JOB);

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectState(States.END)
            .expectStateChanged(7)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.SETUP_JOB,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            setupJobActionMock,
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Handle cancel signal as state machine is preparing to exit SETUP_JOB state.
     *
     * @throws Exception the exception
     */
    @Test
    public void cancelAfterSetup() throws Exception {
        cancelJobAfterEnteringState(States.SETUP_JOB);

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectState(States.END)
            .expectStateChanged(9)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.SETUP_JOB,
            States.LAUNCH_JOB,
            States.MONITOR_JOB,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            setupJobActionMock,
            launchJobActionMock,
            monitorJobActionMock,
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Handle cancel signal as state machine is executing SETUP_JOB state action.
     *
     * @throws Exception the exception
     */
    @Test
    public void cancelDuringSetup() throws Exception {
        ((TestStateAction) setupJobActionMock).cancelJobDuringExecution();

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectState(States.END)
            .expectStateChanged(7)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.SETUP_JOB,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            setupJobActionMock,
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Handle cancel signal as state machine is entering LAUNCH_JOB state.
     *
     * @throws Exception the exception
     */
    @Test
    public void cancelBeforeLaunch() throws Exception {
        cancelJobBeforeEnteringState(States.LAUNCH_JOB);

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectState(States.END)
            .expectStateChanged(9)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.SETUP_JOB,
            States.LAUNCH_JOB,
            States.MONITOR_JOB,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            setupJobActionMock,
            launchJobActionMock,
            monitorJobActionMock,
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Handle cancel signal as state machine is preparing to exit LAUNCH_JOB state.
     *
     * @throws Exception the exception
     */
    @Test
    public void cancelAfterLaunch() throws Exception {
        cancelJobAfterEnteringState(States.LAUNCH_JOB);

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectState(States.END)
            .expectStateChanged(9)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.SETUP_JOB,
            States.LAUNCH_JOB,
            States.MONITOR_JOB,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            setupJobActionMock,
            launchJobActionMock,
            monitorJobActionMock,
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Handle cancel signal as state machine is executing LAUNCH_JOB state action.
     *
     * @throws Exception the exception
     */
    @Test
    public void cancelDuringLaunch() throws Exception {

        ((TestStateAction) launchJobActionMock).cancelJobDuringExecution();

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectState(States.END)
            .expectStateChanged(9)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.SETUP_JOB,
            States.LAUNCH_JOB,
            States.MONITOR_JOB,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            setupJobActionMock,
            launchJobActionMock,
            monitorJobActionMock,
            cleanupJobActionMock,
            shutdownActionMock
        );
    }

    /**
     * Test failure during INITIALIZATION state action.
     *
     * @throws Exception the exception
     */
    @Test
    public void initErrorTest() throws Exception {
        ((TestStateAction) initializeActionMock).makeActionFail();

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectStateChanged(3)
            .expectState(States.END)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.HANDLE_ERROR,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            handleErrorActionMock
        );
    }

    /**
     * Test failure during RESOLVE_JOB_SPECIFICATION state action.
     *
     * @throws Exception the exception
     */
    @Test
    public void resolveJobSpecificationErrorTest() throws Exception {

        ((TestStateAction) resolveJobSpecificationActionMock).makeActionFail();

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectStateChanged(5)
            .expectState(States.END)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.HANDLE_ERROR,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            handleErrorActionMock
        );
    }

    /**
     * Test failure during SHUTDOWN state action.
     *
     * @throws Exception the exception
     */
    @Test
    public void shutdownErrorTest() throws Exception {

        ((TestStateAction) shutdownActionMock).makeActionFail();

        StateMachineTestPlanBuilder.<States, Events>builder()
            .defaultAwaitTime(AWAIT_TIME)
            .stateMachine(sm)
            .step()
            .expectState(States.READY)
            .and()
            .step()
            .sendEvent(Events.START)
            .expectStateChanged(10)
            .expectState(States.END)
            .expectStateMachineStopped(1)
            .and()
            .build()
            .test();

        Assert.assertTrue(sm.isComplete());

        assertStatesVisited(
            States.READY,
            States.INITIALIZE,
            States.CONFIGURE_AGENT,
            States.RESOLVE_JOB_SPECIFICATION,
            States.SETUP_JOB,
            States.LAUNCH_JOB,
            States.MONITOR_JOB,
            States.CLEANUP_JOB,
            States.SHUTDOWN,
            States.HANDLE_ERROR,
            States.END
        );

        assertActionsExecuted(
            initializeActionMock,
            configureAgentActionMock,
            resolveJobSpecificationActionMock,
            setupJobActionMock,
            launchJobActionMock,
            monitorJobActionMock,
            cleanupJobActionMock,
            shutdownActionMock,
            handleErrorActionMock
        );
    }

    private class ExecutionPathListener
        extends StateMachineListenerAdapter<States, Events>
        implements JobExecutionListener {

        @Override
        public void stateEntered(final State<States, Events> state) {
            visitedStates.add(state.getId());
        }

        @Override
        public void onExecute(
            final StateMachine<States, Events> stateMachine,
            final Action<States, Events> action,
            final long duration
        ) {
            executedActions.add(action);
        }
    }

    private void cancelJobBeforeEnteringState(final States targetState) {
        this.sm.getStateMachineAccessor().doWithAllRegions(
            function -> function.addStateMachineInterceptor(
                new StateMachineInterceptorAdapter<States, Events>() {
                    @Override
                    public void preStateChange(
                        final State<States, Events> state,
                        final Message<Events> message,
                        final Transition<States, Events> transition,
                        final StateMachine<States, Events> stateMachine
                    ) {
                        log.info(
                            "Sending event {} before transitioning to state: {}",
                            Events.CANCEL_JOB_LAUNCH,
                            targetState
                        );
                        if (state.getId() == targetState) {
                            stateMachine.sendEvent(Events.CANCEL_JOB_LAUNCH);
                        }
                    }
                }
            )
        );
    }

    private void cancelJobAfterEnteringState(final States targetState) {
        this.sm.getStateMachineAccessor().doWithAllRegions(
            function -> function.addStateMachineInterceptor(
                new StateMachineInterceptorAdapter<States, Events>() {
                    @Override
                    public void postStateChange(
                        final State<States, Events> state,
                        final Message<Events> message,
                        final Transition<States, Events> transition,
                        final StateMachine<States, Events> stateMachine
                    ) {
                        log.info(
                            "Sending event {} after transitioning to state: {}",
                            Events.CANCEL_JOB_LAUNCH,
                            targetState
                        );
                        if (state.getId() == targetState) {
                            stateMachine.sendEvent(Events.CANCEL_JOB_LAUNCH);
                        }
                    }
                }
            )
        );
    }


    private void assertActionsExecuted(final StateAction... expectedStateActions) {
        Assert.assertEquals(
            Lists.newArrayList(expectedStateActions),
            executedActions
        );

        final HashSet<StateAction> expectedStateActionsSet = Sets.newHashSet(expectedStateActions);

        for (Pair<States, StateAction> stateAndAction : statesWithActions) {
            final TestStateAction action = (TestStateAction) stateAndAction.getRight();
            final int expectedExecutionCount = expectedStateActionsSet.contains(action) ? 1 : 0;
            final int actualExecutionCount = action.getExecutionCount();
            Assert.assertEquals(
                "Unexpected executions for state " + stateAndAction.getLeft() + "action",
                expectedExecutionCount,
                actualExecutionCount
            );
        }
    }

    private void assertStatesVisited(final States... expectedStatesVisited) {
        Assert.assertEquals(
            Lists.newArrayList(expectedStatesVisited),
            this.visitedStates
        );
    }

    private static class TestStateAction implements
        StateAction.Initialize,
        StateAction.ConfigureAgent,
        StateAction.ResolveJobSpecification,
        StateAction.SetUpJob,
        StateAction.LaunchJob,
        StateAction.MonitorJob,
        StateAction.CleanupJob,
        StateAction.Shutdown,
        StateAction.HandleError {

        private final Random random;
        private final States state;
        private final Events completionEvent;
        private boolean cancelDuringExecution;
        private boolean failAction;
        private final AtomicInteger executionCount;

        TestStateAction(
            final States state,
            final Events completionEvent
        ) {
            this.state = state;
            this.completionEvent = completionEvent;
            this.random = new Random();
            this.executionCount = new AtomicInteger();
        }

        @Override
        public void execute(final StateContext<States, Events> context) {
            log.info("Executing test action for state {}", state);
            if (MIN_ACTION_DURATION_MS > 0) {
                final int sleepMillis =
                    random.nextInt(MAX_ACTION_DURATION_MS - MIN_ACTION_DURATION_MS) + MIN_ACTION_DURATION_MS;
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (cancelDuringExecution) {
                log.info("Sending CANCEL_JOB_LAUNCH event during action for state {}", state);
                context.getStateMachine().sendEvent(Events.CANCEL_JOB_LAUNCH);
            }

            if (failAction) {
                log.info("Failing test action for state {}", state);
                context.getStateMachine().sendEvent(Events.ERROR);
            } else {
                context.getStateMachine().sendEvent(completionEvent);
            }

            executionCount.incrementAndGet();
        }

        void cancelJobDuringExecution() {
            this.cancelDuringExecution = true;
        }

        void makeActionFail() {
            this.failAction = true;
        }

        int getExecutionCount() {
            return executionCount.get();
        }

        @Override
        public void cleanup() {
        }
    }

}
