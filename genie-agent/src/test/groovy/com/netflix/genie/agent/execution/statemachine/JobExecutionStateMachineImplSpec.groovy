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
package com.netflix.genie.agent.execution.statemachine

import com.netflix.genie.agent.execution.process.JobProcessManager
import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.execution.statemachine.listeners.JobExecutionListener
import com.netflix.genie.agent.execution.statemachine.listeners.LoggingListener
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.atomic.AtomicBoolean

class JobExecutionStateMachineImplSpec extends Specification {
    AtomicBoolean started
    List<ExecutionStage> stages
    ExecutionContext context
    JobExecutionListener loggingListener = new LoggingListener() // Pre installed by default, can be removed
    JobExecutionListener mockListener = Mock(JobExecutionListener) // Pre installed by default, can be removed
    Collection<JobExecutionListener> listeners
    JobExecutionStateMachineImpl sm
    ExecutionStage mockExecutionStage
    JobProcessManager jobProcessManager

    void setup() {
        this.started = new AtomicBoolean(false)
        this.stages = []
        this.context = Mock(ExecutionContext)
        this.listeners = [loggingListener, mockListener]
        this.jobProcessManager = Mock(JobProcessManager)
        this.sm = new JobExecutionStateMachineImpl(stages, context, listeners, jobProcessManager)

        this.mockExecutionStage = Mock(ExecutionStage)
    }

    def "Getters"() {
        expect:
        sm.getExecutionContext() == context
        sm.getExecutionStages() == stages
    }

    def "Run twice"() {

        when:
        sm.run()

        then:
        1 * context.getStarted() >> started

        when:
        sm.run()

        then:
        1 * context.getStarted() >> started
        thrown(IllegalStateException)
    }

    def "Run to completion without retries"() {
        setup:
        States state = States.CLEAN
        stages.addAll([
            new SleepExecutionStage(States.LAUNCH_JOB, 15),
            new SleepExecutionStage(States.WAIT_JOB_COMPLETION, 0),
            mockExecutionStage
        ])

        when:
        sm.run()

        then:
        1 * context.getStarted() >> started
        1 * mockListener.stateMachineStarted()
        1 * mockListener.stateEntered(States.LAUNCH_JOB)
        1 * context.isExecutionAborted() >> false
        1 * mockListener.beforeStateActionAttempt(States.LAUNCH_JOB)
        1 * mockListener.afterStateActionAttempt(States.LAUNCH_JOB, null)
        1 * mockListener.stateExited(States.LAUNCH_JOB)
        1 * mockListener.stateEntered(States.WAIT_JOB_COMPLETION)
        1 * context.isExecutionAborted() >> false
        1 * mockListener.beforeStateActionAttempt(States.WAIT_JOB_COMPLETION)
        1 * mockListener.afterStateActionAttempt(States.WAIT_JOB_COMPLETION, null)
        1 * mockListener.stateExited(States.WAIT_JOB_COMPLETION)
        1 * mockExecutionStage.getState() >> state
        1 * mockListener.stateEntered(state)
        1 * context.isExecutionAborted() >> false
        1 * mockListener.beforeStateActionAttempt(state)
        1 * mockExecutionStage.attemptStageAction(context)
        1 * mockListener.afterStateActionAttempt(state, null)
        1 * mockListener.stateExited(state)
        1 * mockListener.stateEntered(States.DONE)
        1 * mockListener.stateMachineStopped()
    }

    def "Retry until fatal exception, then abort execution"() {
        setup:
        Throwable retryableException = new RetryableJobExecutionException("...", new IOException(""))
        States state = States.HANDSHAKE
        stages.addAll([
            mockExecutionStage
        ])

        when:
        sm.run()

        then:
        1 * context.getStarted() >> started
        1 * mockListener.stateMachineStarted()
        1 * mockExecutionStage.getState() >> state
        1 * mockListener.stateEntered(state)

        // This part repeats for 1 + number of retries
        4 * context.isExecutionAborted() >> false
        4 * mockListener.beforeStateActionAttempt(state)
        4 * mockExecutionStage.attemptStageAction(context) >> { throw retryableException }
        4 * mockListener.afterStateActionAttempt(state, retryableException)
        4 * context.recordTransitionException(state, retryableException)
        1 * mockListener.delayedStateActionRetry(state, 1 * JobExecutionStateMachineImpl.RETRY_DELAY)
        1 * mockListener.delayedStateActionRetry(state, 2 * JobExecutionStateMachineImpl.RETRY_DELAY)
        1 * mockListener.delayedStateActionRetry(state, 3 * JobExecutionStateMachineImpl.RETRY_DELAY)
        1 * mockListener.fatalException(state, _ as FatalJobExecutionException) >> {
            args ->
                assert (args[1] as FatalJobExecutionException).getCause() == retryableException
        }
        1 * context.recordTransitionException(state, _ as FatalJobExecutionException)
        1 * context.isExecutionAborted() >> false
        1 * context.setExecutionAbortedFatalException(_ as FatalJobExecutionException)
        1 * mockListener.executionAborted(state, _ as FatalJobExecutionException)
        1 * mockListener.stateExited(state)
        1 * mockListener.stateEntered(States.DONE)
        1 * mockListener.stateMachineStopped()

        then:
        state.getTransitionRetries() == 3
        state.isCriticalState()
    }

    def "Fatal exception stops retries"() {
        setup:
        Throwable fatalException = new FatalJobExecutionException(States.HANDSHAKE, "...", new IOException())
        States state = States.HANDSHAKE
        stages.addAll([
            mockExecutionStage
        ])

        when:
        sm.run()

        then:
        1 * context.getStarted() >> started
        1 * mockListener.stateMachineStarted()
        1 * mockExecutionStage.getState() >> state
        1 * mockListener.stateEntered(state)

        // This part repeats for 1 + number of retries
        1 * context.isExecutionAborted() >> false
        1 * mockListener.beforeStateActionAttempt(state)
        1 * mockExecutionStage.attemptStageAction(context) >> { throw fatalException }
        1 * mockListener.afterStateActionAttempt(state, fatalException)
        1 * context.recordTransitionException(state, fatalException)
        0 * mockListener.delayedStateActionRetry(state, _ as Long)
        1 * mockListener.fatalException(state, fatalException)
        1 * context.isExecutionAborted() >> false
        1 * context.setExecutionAbortedFatalException(fatalException)
        1 * mockListener.executionAborted(state, fatalException)
        1 * mockListener.stateExited(state)
        1 * mockListener.stateEntered(States.DONE)
        1 * mockListener.stateMachineStopped()

        then:
        state.getTransitionRetries() == 3
        state.isCriticalState()
    }

    def "Unexpected exception is fatal"() {
        setup:
        Throwable exception = new RuntimeException()
        States state = States.HANDSHAKE
        stages.addAll([
            mockExecutionStage
        ])

        when:
        sm.run()

        then:
        1 * context.getStarted() >> started
        1 * mockListener.stateMachineStarted()
        1 * mockExecutionStage.getState() >> state
        1 * mockListener.stateEntered(state)

        // This part repeats for 1 + number of retries
        1 * context.isExecutionAborted() >> false
        1 * mockListener.beforeStateActionAttempt(state)
        1 * mockExecutionStage.attemptStageAction(context) >> { throw exception }
        1 * mockListener.afterStateActionAttempt(state, exception)
        1 * context.recordTransitionException(state, _ as FatalJobExecutionException) >> {
            args ->
                (args[1] as FatalJobExecutionException).getCause() == exception
        }
        0 * mockListener.delayedStateActionRetry(state, _ as Long)
        1 * mockListener.fatalException(state, _ as FatalJobExecutionException)
        1 * context.isExecutionAborted() >> false
        1 * context.setExecutionAbortedFatalException(_ as FatalJobExecutionException)
        1 * mockListener.executionAborted(state, _ as FatalJobExecutionException)
        1 * mockListener.stateExited(state)
        1 * mockListener.stateEntered(States.DONE)
        1 * mockListener.stateMachineStopped()

        then:
        state.getTransitionRetries() == 3
        state.isCriticalState()
    }

    def "Fatal exception in non-critical stage stops abort execution"() {
        setup:
        Throwable exception = new RuntimeException()
        States state = States.ARCHIVE
        stages.addAll([
            mockExecutionStage
        ])

        when:
        sm.run()

        then:
        1 * context.getStarted() >> started
        1 * mockListener.stateMachineStarted()
        1 * mockExecutionStage.getState() >> state
        1 * mockListener.stateEntered(state)

        // This part repeats for 1 + number of retries
        1 * context.isExecutionAborted() >> false
        1 * mockListener.beforeStateActionAttempt(state)
        1 * mockExecutionStage.attemptStageAction(context) >> { throw exception }
        1 * mockListener.afterStateActionAttempt(state, exception)
        1 * context.recordTransitionException(state, _ as FatalJobExecutionException) >> {
            args ->
                (args[1] as FatalJobExecutionException).getCause() == exception
        }
        0 * mockListener.delayedStateActionRetry(state, _ as Long)
        1 * mockListener.fatalException(state, _ as FatalJobExecutionException)
        0 * context.setExecutionAbortedFatalException(_ as FatalJobExecutionException)
        0 * mockListener.executionAborted(state, _ as FatalJobExecutionException)
        1 * mockListener.stateExited(state)
        1 * mockListener.stateEntered(States.DONE)
        1 * mockListener.stateMachineStopped()

        then:
        state.getTransitionRetries() == 0
        !state.isCriticalState()
    }

    def "Non skippable states are executed after execution is aborted"() {
        setup:
        States state = States.ARCHIVE
        stages.addAll([
            mockExecutionStage
        ])

        when:
        sm.run()

        then:
        !state.isSkippedDuringAbortedExecution()
        1 * context.getStarted() >> started
        1 * mockExecutionStage.getState() >> state
        1 * context.isExecutionAborted() >> true
        1 * mockExecutionStage.attemptStageAction(context)
    }

    def "Skippable states are not executed after execution is aborted"() {
        setup:
        States state = States.LAUNCH_JOB
        stages.addAll([
            mockExecutionStage
        ])

        when:
        sm.run()

        then:
        state.isSkippedDuringAbortedExecution()
        1 * context.getStarted() >> started
        1 * mockExecutionStage.getState() >> state
        1 * context.isExecutionAborted() >> true
        0 * mockExecutionStage.attemptStageAction(context)
    }

    def "First fatal exception in critical stage is preserved"() {
        setup:
        States state1 = States.WAIT_JOB_COMPLETION
        States state2 = States.SET_STATUS_FINAL
        ExecutionStage otherMockExecutionStage = Mock(ExecutionStage)
        stages.addAll([mockExecutionStage, otherMockExecutionStage])

        when:
        sm.run()

        then:
        state1.isCriticalState()
        !state1.isSkippedDuringAbortedExecution()
        state2.isCriticalState()
        !state2.isSkippedDuringAbortedExecution()

        1 * context.getStarted() >> started
        1 * mockExecutionStage.getState() >> state1
        2 * context.isExecutionAborted() >> false
        1 * mockExecutionStage.attemptStageAction(context) >> { throw new RuntimeException() }
        1 * context.recordTransitionException(state1, _ as FatalJobExecutionException)
        1 * context.setExecutionAbortedFatalException(_ as FatalJobExecutionException)
        1 * otherMockExecutionStage.getState() >> state2
        2 * context.isExecutionAborted() >> true
        1 * otherMockExecutionStage.attemptStageAction(context) >> { throw new RuntimeException() }
        1 * context.recordTransitionException(state2, _ as FatalJobExecutionException)
        0 * context.setExecutionAbortedFatalException(_ as FatalJobExecutionException)
    }

    def "Abort via kill only executes non-skip states"() {
        setup:
        States state1 = States.DOWNLOAD_DEPENDENCIES
        States state2 = States.SET_STATUS_FINAL
        ExecutionStage otherMockExecutionStage = Mock(ExecutionStage)
        stages.addAll([mockExecutionStage, otherMockExecutionStage])

        when:
        sm.kill(KillService.KillSource.TIMEOUT)
        sm.run()

        then:
        1 * jobProcessManager.kill(KillService.KillSource.TIMEOUT)
        state1.isSkippedDuringAbortedExecution()
        !state2.isSkippedDuringAbortedExecution()

        1 * context.getStarted() >> started
        1 * mockExecutionStage.getState() >> state1
        1 * context.isExecutionAborted() >> true
        0 * mockExecutionStage.attemptStageAction(context)
        1 * otherMockExecutionStage.getState() >> state2
        1 * context.isExecutionAborted() >> true
        1 * otherMockExecutionStage.attemptStageAction(context)
    }

    @Unroll
    def "Handle kill (#source)"() {
        when:
        sm.kill(source)

        then:
        1 * context.setJobKilled(true)
        1 * jobProcessManager.kill(source)
        if (source == KillService.KillSource.REMOTE_STATUS_MONITOR) {
            1 * context.setSkipFinalStatusUpdate(true)
        } else {
            0 * context.setSkipFinalStatusUpdate(_)
        }

        where:
        source                                       | _
        KillService.KillSource.SYSTEM_SIGNAL         | _
        KillService.KillSource.API_KILL_REQUEST      | _
        KillService.KillSource.TIMEOUT               | _
        KillService.KillSource.FILES_LIMIT           | _
        KillService.KillSource.REMOTE_STATUS_MONITOR | _
    }

    // A stage that takes a certain amount of time to complete its action.
    private class SleepExecutionStage extends ExecutionStage {
        int actionDuration

        protected SleepExecutionStage(States state, int actionDuration) {
            super(state)
            this.actionDuration = actionDuration
        }

        @Override
        protected void attemptStageAction(final ExecutionContext executionContext) throws RetryableJobExecutionException, FatalJobExecutionException {
            sleep(actionDuration)
        }
    }
}
