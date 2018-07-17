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
package com.netflix.genie.common.exceptions;

import lombok.Getter;

/**
 * Extension of a GenieException for a user exceeding some limit (e.g., submitting too many jobs).
 *
 * @author mprimi
 * @since 3.1.0
 */
@Getter
public final class GenieUserLimitExceededException extends GenieException {

    private static final String ACTIVE_JOBS_LIMIT = "activeJobs";
    private final String user;
    private final String exceededLimitName;

    /**
     * Constructor.
     *
     * @param user      user name
     * @param limitName limit name
     * @param message   message
     */
    public GenieUserLimitExceededException(
        final String user,
        final String limitName,
        final String message
    ) {
        super(429, message);
        this.user = user;
        this.exceededLimitName = limitName;
    }

    /**
     * Static factory method to produce a GenieUserLimitExceededException suitable for when the user exceeded the
     * maximum number of active jobs and its trying to submit yet another.
     *
     * @param user            the user name
     * @param activeJobsCount the count of active jobs for this user
     * @param activeJobsLimit the current limit on active jobs
     * @return a new GenieUserLimitExceededException
     */
    public static GenieUserLimitExceededException createForActiveJobsLimit(
        final String user,
        final long activeJobsCount,
        final long activeJobsLimit
    ) {
        return new GenieUserLimitExceededException(
            user,
            ACTIVE_JOBS_LIMIT,
            "User exceeded active jobs limit ("
                + activeJobsCount
                + "/"
                + activeJobsLimit
                + ")"
        );
    }
}
