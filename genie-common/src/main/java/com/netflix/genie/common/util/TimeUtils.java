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
package com.netflix.genie.common.util;

import java.time.Duration;
import java.util.Date;

/**
 * Utility methods for dealing with time. Particularly duration.
 *
 * @author tgianos
 * @since 3.0.0
 */
public final class TimeUtils {

    /**
     * Protected constructor for utility class.
     */
    protected TimeUtils() {
    }

    /**
     * Get the duration between when something was started and finished.
     *
     * @param started  The start time. Can be null will automatically set the duration to 0
     * @param finished The finish time. If null will use (current time - started time) to get the duration
     * @return The duration or zero if no duration
     */
    public static Duration getDuration(final Date started, final Date finished) {
        if (started == null || started.getTime() == 0) {
            // Never started
            return Duration.ZERO;
        } else if (finished == null || finished.getTime() == 0) {
            // Started but hasn't finished
            return Duration.ofMillis(new Date().getTime() - started.getTime());
        } else {
            // Started and finished
            return Duration.ofMillis(finished.getTime() - started.getTime());
        }
    }
}
