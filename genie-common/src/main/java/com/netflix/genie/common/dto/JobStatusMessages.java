/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.common.dto;

/**
 * Constant strings for status message attached to a job after it terminates.
 *
 * @author mprimi
 * @since 3.0.7
 */
public final class JobStatusMessages {
    //TODO this class could go away and we could fold this into JobStatus

    /**
     * The message for when the agent is preparing to launch the job.
     */
    public static final String JOB_INITIALIZING = "Preparing to launch job";

    /**
     * The status message while a job is actively running.
     */
    public static final String JOB_RUNNING = "Job is Running.";

    /**
     * Job killed because maximum run time was exceeded.
     */
    public static final String JOB_EXCEEDED_TIMEOUT = "Job exceeded timeout.";

    /**
     * Job killed because a limit related to the size of the directory was exceeded.
     */
    public static final String JOB_EXCEEDED_FILES_LIMIT = "Job files exceeded limit.";

    /**
     * Job killed by user.
     */
    public static final String JOB_KILLED_BY_USER = "Job was killed by user.";

    /**
     * Job killed by system.
     */
    public static final String JOB_KILLED_BY_SYSTEM = "Job was killed by system.";

    /**
     * Job completed successfully.
     */
    public static final String JOB_FINISHED_SUCCESSFULLY = "Job finished successfully.";

    /**
     * Job terminated with non-zero exit code.
     */
    public static final String JOB_FAILED = "Job failed.";

    /**
     * Job failed in in the setup portion of the run script (setting environment or running resources setup scripts).
     */
    public static final String JOB_SETUP_FAILED =
        "Job script failed during setup. See setup log for details";

    /**
     * Agent failed to claim job id, job record does not exist, it is not in a state where it can be claimed.
     */
    public static final String FAILED_TO_CLAIM_JOB = "Failed to claim job";

    /**
     * The agent failed to create a directory for the job.
     */
    public static final String FAILED_TO_CREATE_JOB_DIRECTORY = "Failed to create job directory";

    /**
     * The agent failed to compose or write a job script.
     */
    public static final String FAILED_TO_CREATE_JOB_SCRIPT = "Failed to create job script";

    /**
     * The agent failed to download dependencies. They may not exist, or the agent may not have the required permission
     * to read them or write them on local disk.
     */
    public static final String FAILED_TO_DOWNLOAD_DEPENDENCIES = "Failed to download dependencies";

    /**
     * The server refused to let the agent run a job, refusal is based on agent version or hostname.
     */
    public static final String FAILED_AGENT_SERVER_HANDSHAKE = "Failed agent/server handshake";

    /**
     * The agent encountered an error while attempting to launch the job process.
     */
    public static final String FAILED_TO_LAUNCH_JOB = "Failed to launch job";

    /**
     * The agent failed to reserve the job id, possibly because it is already in use.
     */
    public static final String FAILED_TO_RESERVE_JOB_ID = "Failed to reserve job id";

    /**
     * The agent encountered an unhandled error while waiting for the job process to complete.
     */
    public static final String FAILED_TO_WAIT_FOR_JOB_COMPLETION = "Failed to wait for job completion";

    /**
     * The agent could not determine what state the job is in.
     */
    public static final String UNKNOWN_JOB_STATE = "Unknown job state";

    /**
     * The agent failed to update the persisted job status via server.
     */
    public static final String FAILED_TO_UPDATE_STATUS = "Failed to update job status";

    /**
     * The agent failed to start up (before any job-specific action is taken).
     */
    public static final String FAILED_AGENT_STARTUP = "Failed to initialize agent";

    /**
     * The agent could not retrieve the job specifications.
     * Could mean it does not exist or cannot be resolved.
     */
    public static final String FAILED_TO_OBTAIN_JOB_SPECIFICATION = "Failed to obtain job specification";

    /**
     * The agent failed to initialize a required runtime service (file stream, heartbeat, kill).
     */
    public static final String FAILED_TO_START_SERVICE = "Failed to start service";

    /**
     * The agent failed to configure itself, probably due to invalid command-line arguments.
     */
    public static final String FAILED_AGENT_CONFIGURATION = "Failed to configure execution";

    /**
     * Could not resolve the job.
     */
    public static final String FAILED_TO_RESOLVE_JOB
        = "Failed to resolve job given original request and available resources";

    /**
     * While job was running, server status was set to something else (probably marked failed by the leader after
     * the agent was disconnected for too long).
     */
    public static final String JOB_MARKED_FAILED = "The job status changed server-side while the job was running";

    /**
     * Job resolution fails due to runtime error (i.e. NOT due to unsatisfiable constraints).
     */
    public static final String RESOLUTION_RUNTIME_ERROR = "Runtime error during job resolution";

    /**
     * Private constructor, this class is not meant to be instantiated.
     */
    private JobStatusMessages() {
    }
}
