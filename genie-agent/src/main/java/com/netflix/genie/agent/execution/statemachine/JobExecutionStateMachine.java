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
import org.springframework.context.ApplicationListener;

/**
 * Interface JobExecutionStateMachine.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface JobExecutionStateMachine extends ApplicationListener<KillService.KillEvent> {

    /**
     * Starts the state machine and returns.
     */
    void start();

    /**
     * Waits for the state machine to stop executing.
     *
     * @return the final state in which the machine stopped
     * @throws InterruptedException if the waiting thread is interrupted
     */
    States waitForStop() throws InterruptedException;

    /**
     * Force an early termination of the state machine, in response to user submitting a kill via API or ctrl-c.
     * Notice:
     *  - State actions are not interrupted. Only after the one currently executing is completed the stop
     *    signal is processed (TODO: make long-running actions, such as setup, interruptible).
     *  - Some actions are still performed before the program exits. For example publish the updated job status
     *    server-side. Refer to the {@link StateMachineConfig} for details.
     */
    void stop();
}
