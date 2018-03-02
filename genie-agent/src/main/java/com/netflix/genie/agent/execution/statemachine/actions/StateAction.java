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

package com.netflix.genie.agent.execution.statemachine.actions;

import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.agent.execution.statemachine.States;
import org.springframework.statemachine.action.Action;

/**
 * Interface types for individual state actions.
 * @author mprimi
 * @since 4.0.0
 */
public interface StateAction extends Action<States, Events> {

    /**
     * Clean up any cruft create by the action once the job has executed (or aborted).
     */
    void cleanup();

    /**
     * INITIALIZE state action interface.
     */
    interface Initialize extends StateAction {
    }

    /**
     * CONFIGURE_AGENT state action interface.
     */
    interface ConfigureAgent extends StateAction {
    }

    /**
     * RESOLVE_JOB_SPECIFICATION state action interface.
     */
    interface ResolveJobSpecification extends StateAction {
    }

    /**
     * SETUP_JOB state action interface.
     */
    interface SetUpJob extends StateAction {
    }

    /**
     * LAUNCH_JOB state action interface.
     */
    interface LaunchJob extends StateAction {
    }

    /**
     * MONITOR_JOB state action interface.
     */
    interface MonitorJob extends StateAction {
    }

    /**
     * CLEANUP_JOB state action interface.
     */
    interface CleanupJob extends StateAction {
    }

    /**
     * SHUTDOWN state action interface.
     */
    interface Shutdown extends StateAction {
    }

    /**
     * HANDLE_ERROR state action interface.
     */
    interface HandleError extends StateAction {
    }
}
