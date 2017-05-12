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
 */
public final class JobStatusMessage {

    /**
     * Job killed because maximum stdout lenght was exceeded.
     */
    public static final String JOB_EXCEEDED_STDOUT_LENGTH = "Std out length exceeded.";

    /**
     * Job killed because maximum stderr lenght was exceeded.
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
     * Private constructor, this class is not meant to be instantiated.
     */
    private JobStatusMessage() {
    }
}
