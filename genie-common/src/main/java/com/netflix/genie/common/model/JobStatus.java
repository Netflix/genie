/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.wordnik.swagger.annotations.ApiModel;
import org.apache.commons.lang3.StringUtils;


/**
 * Possible statuses for a Job.
 *
 * @author tgianos
 */
@ApiModel(value = "Available statuses for a job")
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
    FAILED;

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
                "Unacceptable job status. Must be one of {Init, Running, Succeeded, Killed, Failed}");
    }
}
