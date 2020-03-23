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

import com.netflix.genie.agent.execution.services.KillService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.listener.StateMachineListenerAdapter;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Implementation of JobExecutionStateMachine.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class JobExecutionStateMachineImpl implements JobExecutionStateMachine {

    @Getter
    private final StateMachine<States, Events> stateMachine;
    private final StateMachineListenerAdapter<States, Events> executionCompletionListener;
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private final ExecutionContext executionContext;
    @Getter
    private final List<ExecutionStage> executionStages;

    /**
     * Constructor.
     *
     * @param stateMachine     the state machine
     * @param executionContext the execution context
     * @param executionStages  the execution stages
     */
    public JobExecutionStateMachineImpl(
        final StateMachine<States, Events> stateMachine,
        final ExecutionContext executionContext,
        final List<ExecutionStage> executionStages
    ) {
        this.stateMachine = stateMachine;
        this.executionStages = executionStages;
        this.executionContext = executionContext;
        this.stateMachine
            .getExtendedState()
            .getVariables()
            .put(ExecutionStage.EXECUTION_CONTEXT_CONTEXT_KEY, executionContext);
        this.executionCompletionListener = new StateMachineListenerAdapter<States, Events>() {
            @Override
            public void stateMachineStopped(final StateMachine<States, Events> sm) {
                log.debug("State machine execution complete");
                completionLatch.countDown();
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        // Listen for state machine stop event
        stateMachine.addStateListener(executionCompletionListener);
        // Start the state machine execution
        stateMachine.start();
        // Kick off first transition
        stateMachine.sendEvent(Events.START);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public States waitForStop() throws InterruptedException {
        try {
            completionLatch.await();
            return stateMachine.getState().getId();
        } catch (final InterruptedException e) {
            log.error("Interrupted while waiting for state machine to complete");
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        log.info("Stopping state machine (in state: {})", stateMachine.getState().getId());
        // Mark the job as killed and wait for the state machine to do its job
        this.executionContext.setJobKilled(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onApplicationEvent(final KillService.KillEvent event) {
        log.info("Stopping state machine due to kill event (source: {})", event.getKillSource());
        this.stop();
    }
}
