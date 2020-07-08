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

import java.util.List;

/**
 * Interface JobExecutionStateMachine hides the actual state machine details.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface JobExecutionStateMachine {

    /**
     * Runs the state machine until completion.
     */
    void run();

    /**
     * Get the list of execution stages.
     *
     * @return an immutable ordered list of execution stages
     */
    List<ExecutionStage> getExecutionStages();

    /**
     * Get the execution context.
     * This is meant for post-execution read-only access.
     *
     * @return the execution context used by the state machine.
     */
    ExecutionContext getExecutionContext();

    /**
     * Abort execution, if necessary by stopping the running job process.
     * The state machine still runs to termination, but may skip steps as appropriate.
     *
     * @param killSource a representation of who/what requested the kill
     */
    void kill(KillService.KillSource killSource);
}
