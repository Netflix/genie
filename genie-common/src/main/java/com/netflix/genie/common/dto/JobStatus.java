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

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.apache.commons.lang3.StringUtils;

/**
 * Possible statuses for a Job.
 *
 * @author tgianos
 */
public enum JobStatus {

    /**
     * Job has been initialized, but not running yet.
     */
    INIT,
    /**
     * Job is now running.
     */
    RUNNING,
    /**
     * Job has finished executing, and is successful.
     */
    SUCCEEDED,
    /**
     * Job has been killed.
     */
    KILLED,
    /**
     * Job failed.
     */
    FAILED,
    /**
     * Job cannot be run due to invalid criteria.
     */
    INVALID;

    /**
     * Parse job status.
     *
     * @param value string to parse/convert
     * @return INIT, RUNNING, SUCCEEDED, KILLED, FAILED if match
     * @throws GeniePreconditionException if invalid value passed in
     */
    public static JobStatus parse(final String value) throws GeniePreconditionException {
        if (StringUtils.isNotBlank(value)) {
            for (final JobStatus status : JobStatus.values()) {
                if (value.equalsIgnoreCase(status.toString())) {
                    return status;
                }
            }
        }
        throw new GeniePreconditionException(
            "Unacceptable job status. Must be one of {Init, Running, Succeeded, Killed, Failed, Invalid}"
        );
    }

    /**
     * Check whether this job is in an active state or not.
     *
     * @return True if the job is still actively processing in some manner
     */
    public boolean isActive() {
        return this == INIT || this == RUNNING;
    }

    /**
     * Check whether the job is no longer running.
     *
     * @return True if the job is no longer processing for one reason or another.
     */
    public boolean isFinished() {
        return this == SUCCEEDED || this == KILLED || this == FAILED || this == INVALID;
    }
}
