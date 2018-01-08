/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.tasks;

import com.netflix.genie.web.jobs.JobConstants;

import javax.validation.constraints.NotNull;
import java.util.Calendar;

/**
 * Utility methods used by various Genie tasks.
 *
 * @author tgianos
 * @since 3.0.0
 */
public final class TaskUtils {

    /**
     * Protected constructor for utility class.
     */
    protected TaskUtils() {
    }

    /**
     * Get exactly midnight (00:00:00.000) UTC for the current day.
     *
     * @return 12 AM UTC.
     */
    public static Calendar getMidnightUTC() {
        final Calendar cal = Calendar.getInstance(JobConstants.UTC);
        // Make sure everything but the year, month, day is 0
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }

    /**
     * Subtract the given number of days from the given date.
     *
     * @param cal  The calendar object to modify
     * @param days the number of days. If negative they won't be added they will just be subtracted.
     */
    public static void subtractDaysFromDate(@NotNull final Calendar cal, final int days) {
        cal.add(Calendar.DAY_OF_YEAR, days < 0 ? days : days * -1);
    }
}
