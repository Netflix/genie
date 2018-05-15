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

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Possible statuses for a Job.
 *
 * @author tgianos
 */
public enum JobStatus {

    /**
     * The id of the job has been reserved.
     */
    RESERVED(true),
    /**
     * The job specification has been resolved.
     */
    RESOLVED(true),
    /**
     * The job has been accepted by the system via the REST API.
     */
    ACCEPTED(true),
    /**
     * The job has been claimed by a running agent.
     */
    CLAIMED(true),
    /**
     * Job has been initialized, but not running yet.
     */
    INIT(true),
    /**
     * Job is now running.
     */
    RUNNING(true),
    /**
     * Job has finished executing, and is successful.
     */
    SUCCEEDED(false),
    /**
     * Job has been killed.
     */
    KILLED(false),
    /**
     * Job failed.
     */
    FAILED(false),
    /**
     * Job cannot be run due to invalid criteria.
     */
    INVALID(false);

    private static final Set<JobStatus> ACTIVE_STATUSES = Collections.unmodifiableSet(
        Arrays.stream(JobStatus.values()).filter(JobStatus::isActive).collect(Collectors.toSet())
    );

    private static final Set<JobStatus> FINISHED_STATUSES = Collections.unmodifiableSet(
        Arrays.stream(JobStatus.values()).filter(JobStatus::isFinished).collect(Collectors.toSet())
    );

    private final boolean active;

    /**
     * Constructor.
     *
     * @param isActive whether this status should be considered active or not
     */
    JobStatus(final boolean isActive) {
        this.active = isActive;
    }

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
     * Get an unmodifiable set of all the statuses that make up a job being considered active.
     *
     * @return Unmodifiable set of all active statuses
     */
    public static Set<JobStatus> getActiveStatuses() {
        return ACTIVE_STATUSES;
    }

    /**
     * Get an unmodifiable set of all the statuses that make up a job being considered finished.
     *
     * @return Unmodifiable set of all finished statuses
     */
    public static Set<JobStatus> getFinishedStatuses() {
        return FINISHED_STATUSES;
    }

    /**
     * Check whether this job is in an active state or not.
     *
     * @return True if the job is still actively processing in some manner
     */
    public boolean isActive() {
        return this.active;
    }

    /**
     * Check whether the job is no longer running.
     *
     * @return True if the job is no longer processing for one reason or another.
     */
    public boolean isFinished() {
        return !this.active;
    }
}
