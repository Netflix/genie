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

/**
 * JobExecutionStateMachine states.
 */
public enum States {
    /**
     * Initial pseudo-state (no action).
     */
    READY,

    /**
     * Initialize.
     */
    INITIALIZE,

    /**
     * Configure agent.
     */
    CONFIGURE_AGENT,

    /**
     * Resolve job specification.
     */
    RESOLVE_JOB_SPECIFICATION,

    /**
     * Set up job.
     */
    SETUP_JOB,

    /**
     * Launch job.
     */
    LAUNCH_JOB,

    /**
     * Monitor job process.
     */
    MONITOR_JOB,

    /**
     * Clean up after job.
     */
    CLEANUP_JOB,

    /**
     * Shut down agent.
     */
    SHUTDOWN,

    /**
     * Handle error.
     */
    HANDLE_ERROR,

    /**
     * Final pseudo-state (no action).
     */
    END,
}
