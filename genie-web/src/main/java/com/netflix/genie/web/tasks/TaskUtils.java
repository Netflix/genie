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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

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
    public static Instant getMidnightUTC() {
        return ZonedDateTime.now(ZoneId.of("UTC")).withHour(0).withMinute(0).withSecond(0).withNano(0).toInstant();
    }
}
