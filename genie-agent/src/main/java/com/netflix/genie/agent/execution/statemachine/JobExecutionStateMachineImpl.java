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
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.listener.StateMachineListenerAdapter;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

/**
 * Implementation of JobExecutionStateMachine.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Component
@Lazy
@Slf4j
class JobExecutionStateMachineImpl implements JobExecutionStateMachine {

    private final StateMachine<States, Events> stateMachine;
    private final StateMachineListenerAdapter<States, Events> executionCompletionListener;
    private final CountDownLatch completionLatch = new CountDownLatch(1);

    JobExecutionStateMachineImpl(final StateMachine<States, Events> stateMachine) {
        this.stateMachine = stateMachine;
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

    @Override
    public void stop() {
        log.info("Stopping state machine (in state: {})", stateMachine.getState().getId());
        // This event is processed iff the state machine has not reached the state where the job is launched.
        stateMachine.sendEvent(Events.CANCEL_JOB_LAUNCH);
        // If the job is already running, then the machine can do nothing but wait for it to complete
        // (killing the child process is handled elsewhere).
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
