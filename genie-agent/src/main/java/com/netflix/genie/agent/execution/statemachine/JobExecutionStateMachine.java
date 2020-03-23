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
 * Interface JobExecutionStateMachine hides the actual state machine details.
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
     * Waits for the state machine to stop executing (i.e. reach a terminal state).
     *
     * @return the final state in which the machine stopped
     * @throws InterruptedException if the waiting thread is interrupted
     */
    States waitForStop() throws InterruptedException;

    /**
     * Request early termination of the state machine, for example in response to user submitting a kill via API or
     * ctrl-c.
     * <p>
     * Notice:
     * - Transition actions are not interrupted. Shutdown procedure will start after the currently executing transition
     *   action has completed
     * - Some transition actions are still performed before the program exits. For example publish the updated final job
     *   status server-side, or archiving logs.
     */
    void stop();
}
