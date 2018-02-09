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
 * Job execution state machine events.
 */
public enum Events {

    /**
     * Start the state machine.
     */
    START,

    /**
     * Initialization completed successfully.
     */
    INITIALIZE_COMPLETE,

    /**
     * Agent configuration completed successfully.
     */
    CONFIGURE_AGENT_COMPLETE,

    /**
     * Job specification resolution completed successfully.
     */
    RESOLVE_JOB_SPECIFICATION_COMPLETE,

    /**
     * Job setup completed successfully.
     */
    SETUP_JOB_COMPLETE,

    /**
     * Job launch completed successfully.
     */
    LAUNCH_JOB_COMPLETE,

    /**
     * Job executed and completed (regardless of exit status).
     */
    MONITOR_JOB_COMPLETE,

    /**
     * Job cleanup completed successfully.
     */
    CLEANUP_JOB_COMPLETE,

    /**
     * Agent shutdown completed successfully.
     */
    SHUTDOWN_COMPLETE,

    /**
     * User request job launch cancelled before launch.
     */
    CANCEL_JOB_LAUNCH,

    /**
     * Handling of error completed successfully.
     */
    HANDLE_ERROR_COMPLETE,

    /**
     * A state action failed to execute.
     */
    ERROR,
}
