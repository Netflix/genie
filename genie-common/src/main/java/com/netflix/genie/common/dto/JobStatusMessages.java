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
     * Job killed because maximum stdout length was exceeded.
     */
    public static final String JOB_EXCEEDED_STDOUT_LENGTH = "Std out length exceeded.";

    /**
     * Job killed because maximum stderr length was exceeded.
     */
    public static final String JOB_EXCEEDED_STDERR_LENGTH = "Std err length exceeded.";

    /**
     * Job killed because maximum run time was exceeded.
     */
    public static final String JOB_EXCEEDED_TIMEOUT = "Job exceeded timeout.";

    /**
     * Job killed by user.
     */
    public static final String JOB_KILLED_BY_USER = "Job was killed by user.";

    /**
     * Job completed successfully.
     */
    public static final String JOB_FINISHED_SUCCESSFULLY = "Job finished successfully.";

    /**
     * Job terminated with non-zero exit code.
     */
    public static final String JOB_FAILED = "Job failed.";

    /**
     * Job killed, exit status unknown as done file is unreadable.
     */
    public static final String COULD_NOT_LOAD_DONE_FILE = "Failed to load done file.";

    /**
     * Job killed, could not check on the process.
     */
    public static final String JOB_PROCESS_NOT_FOUND = "Couldn't check job process status.";

    /**
     * Job PID terminated, exist status is yet to be verified.
     */
    public static final String PROCESS_DETECTED_TO_BE_COMPLETE = "Process detected to be complete";

    /**
     * Job is undefined intermediate state caused by a crash during setup.
     */
    public static final String SYSTEM_CRASHED_WHILE_JOB_STARTING = "System crashed while job starting";

    /**
     * Job was launched before Genie stopped, and it cannot be re-attached after restart.
     */
    public static final String UNABLE_TO_RE_ATTACH_ON_STARTUP = "Unable to re-attach on startup";

    /**
     * Job was killed by user before even starting.
     */
    public static final String USER_REQUESTED_JOB_BE_KILLED_DURING_INITIALIZATION =
        "User requested job be killed during initialization";

    /**
     * Job precondition was not satisfied during initialization.
     */
    public static final String SUBMIT_PRECONDITION_FAILURE =
        "Job validation failed, further details available in the job output directory";

    /**
     * Job failed with unexpected exception during initialization.
     */
    public static final String SUBMIT_INIT_FAILURE =
        "Job initialization failed, further details available in the job output directory";

    /**
     * Private constructor, this class is not meant to be instantiated.
     */
    private JobStatusMessages() {
    }
}
