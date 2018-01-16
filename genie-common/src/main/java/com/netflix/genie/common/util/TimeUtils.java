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

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;

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
    private TimeUtils() {
    }

    /**
     * Get the duration between when something was started and finished.
     *
     * @param started  The start time. Can be null will automatically set the duration to 0
     * @param finished The finish time. If null will use (current time - started time) to get the duration
     * @return The duration or zero if no duration
     */
    public static Duration getDuration(@Nullable final Instant started, @Nullable final Instant finished) {
        if (started == null || started.toEpochMilli() == 0L) {
            // Never started
            return Duration.ZERO;
        } else if (finished == null || finished.toEpochMilli() == 0L) {
            // Started but hasn't finished
            return Duration.ofMillis(Instant.now().toEpochMilli() - started.toEpochMilli());
        } else {
            // Started and finished
            return Duration.ofMillis(finished.toEpochMilli() - started.toEpochMilli());
        }
    }
}
