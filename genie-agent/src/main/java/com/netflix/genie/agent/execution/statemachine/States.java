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
package com.netflix.genie.agent.execution.statemachine;

import com.netflix.genie.common.dto.JobStatusMessages;
import lombok.Getter;

import javax.validation.constraints.Min;

/**
 * Execution state machine states.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
public enum States {
    /**
     * Initial state, NOOP, before execution to start.
     */
    READY(0, false),

    /**
     * Initialize the agent.
     */
    INITIALIZE_AGENT(0, true, JobStatusMessages.FAILED_AGENT_STARTUP),

    /**
     * Perform agent/server handshake.
     */
    HANDSHAKE(3, true, JobStatusMessages.FAILED_AGENT_SERVER_HANDSHAKE),

    /**
     * Configure the agent based on server-provided values.
     */
    CONFIGURE_AGENT(1, true),

    /**
     * Configure the execution based on command-line arguments.
     */
    CONFIGURE_EXECUTION(0, true, JobStatusMessages.FAILED_AGENT_CONFIGURATION),

    /**
     * Reserve the job id.
     */
    RESERVE_JOB_ID(3, true, JobStatusMessages.FAILED_TO_RESERVE_JOB_ID),

    /**
     * Obtain the job specification.
     */
    OBTAIN_JOB_SPECIFICATION(3, true, JobStatusMessages.FAILED_TO_OBTAIN_JOB_SPECIFICATION),

    /**
     * Claim the job for execution.
     */
    CLAIM_JOB(3, true, JobStatusMessages.FAILED_TO_CLAIM_JOB),

    /**
     * Start heartbeat service.
     */
    START_HEARTBEAT_SERVICE(0, true, JobStatusMessages.FAILED_TO_START_SERVICE),

    /**
     * Start kill service.
     */
    START_KILL_SERVICE(0, true, JobStatusMessages.FAILED_TO_START_SERVICE),

    /**
     * Create the job directory.
     */
    CREATE_JOB_DIRECTORY(0, true, JobStatusMessages.FAILED_TO_CREATE_JOB_DIRECTORY),

    /**
     * Relocate the agent log inside the job directory.
     */
    RELOCATE_LOG(0, true),

    /**
     * Start file stream service.
     */
    START_FILE_STREAM_SERVICE(0, true, JobStatusMessages.FAILED_TO_START_SERVICE),

    /**
     * Update the job status to INIT.
     */
    SET_STATUS_INIT(3, true, JobStatusMessages.FAILED_TO_UPDATE_STATUS),

    /**
     * Create the job script (a.k.a. 'run' file).
     */
    CREATE_JOB_SCRIPT(0, true, JobStatusMessages.FAILED_TO_CREATE_JOB_SCRIPT),

    /**
     * Download dependencies.
     */
    DOWNLOAD_DEPENDENCIES(0, true, JobStatusMessages.FAILED_TO_DOWNLOAD_DEPENDENCIES),

    /**
     * Launch the job process.
     */
    LAUNCH_JOB(0, true, JobStatusMessages.FAILED_TO_LAUNCH_JOB),

    /**
     * Update the job status to RUNNING.
     */
    SET_STATUS_RUNNING(3, true, JobStatusMessages.FAILED_TO_UPDATE_STATUS),

    /**
     * Wait for job completion.
     */
    WAIT_JOB_COMPLETION(0, false, JobStatusMessages.FAILED_TO_WAIT_FOR_JOB_COMPLETION),

    /**
     * Final update to the job status.
     */
    SET_STATUS_FINAL(3, false, JobStatusMessages.FAILED_TO_UPDATE_STATUS),

    /**
     * Stop the kill service.
     */
    STOP_KILL_SERVICE(0, false),

    /**
     * Dump execution error summary in the log.
     */
    LOG_EXECUTION_ERRORS(0, false),

    /**
     * Archive job outputs and logs.
     */
    ARCHIVE(0, false),

    /**
     * Stop the heartbeat service.
     */
    STOP_HEARTBEAT_SERVICE(0, false),

    /**
     * Stop the file stream service.
     */
    STOP_FILES_STREAM_SERVICE(0, false),

    /**
     * Clean job directory post-execution.
     */
    CLEAN(0, false),

    /**
     * Shutdown execution state machine.
     */
    SHUTDOWN(0, false),

    /**
     * Final stage.
     */
    DONE(0, false),

    /**
     * Determine the job final status to publish to server.
     */
    DETERMINE_FINAL_STATUS(0, false, JobStatusMessages.UNKNOWN_JOB_STATE),

    /**
     * Trigger a job directory manifest refresh after the job has launched.
     */
    POST_LAUNCH_MANIFEST_REFRESH(0, true),

    /**
     * Trigger a job directory manifest refresh after job setup completed.
     */
    POST_SETUP_MANIFEST_REFRESH(0, true),

    /**
     * Trigger a job directory manifest refresh after job process terminated.
     */
    POST_EXECUTION_MANIFEST_REFRESH(0, false);

    /**
     * Initial pseudo-state.
     */
    public static final States INITIAL_STATE = READY;

    /**
     * Final pseudo-state.
     */
    public static final States FINAL_STATE = DONE;

    /**
     * If a state is critical, then upon encountering a {@link FatalJobExecutionException} while in it, execution is
     * aborted. Whereas for non critical stage execution is not aborted.
     */
    private final boolean criticalState;

    /**
     * Determines whether the state transition action is performed or skipped in case execution has been previously
     * aborted (due to a kill request, or due to a fatal error).
     */
    private final boolean skippedDuringAbortedExecution;

    /**
     * The final status message sent to the server when a {@link States#criticalState} state fails.
     * Not necessary for states that are not required, since they won't cause the job to fail.
     * These message constants should come from {@link JobStatusMessages}.
     */
    private final String fatalErrorStatusMessage;

    /**
     * Number of retries (i.e. does not include initial attempt) in case a state transition throws
     * {@link RetryableJobExecutionException}.
     */
    @Min(0)
    private final int transitionRetries;

    States(
        final boolean criticalState,
        final boolean skippedDuringAbortedExecution,
        @Min(0) final int transitionRetries,
        final String fatalErrorStatusMessage
    ) {
        this.criticalState = criticalState;
        this.skippedDuringAbortedExecution = skippedDuringAbortedExecution;
        this.transitionRetries = transitionRetries;
        this.fatalErrorStatusMessage = fatalErrorStatusMessage;
    }

    States(
        final int transitionRetries,
        final boolean skippedDuringAbortedExecution,
        final String fatalErrorStatusMessage
    ) {
        this(true, skippedDuringAbortedExecution, transitionRetries, fatalErrorStatusMessage);
    }

    States(
        final int transitionRetries,
        final boolean skippedDuringAbortedExecution
    ) {
        this(false, skippedDuringAbortedExecution, transitionRetries, "Unexpected fatal error in non-critical state");
    }
}
