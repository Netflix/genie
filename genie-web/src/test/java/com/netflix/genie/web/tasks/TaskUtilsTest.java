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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Unit tests for the utility methods for task.
 *
 * @author tgianos
 * @since 3.0.0
 */
class TaskUtilsTest {

    /**
     * Make sure that since we're in the same package we can construct.
     */
    @Test
    void canConstruct() {
        Assertions.assertThat(new TaskUtils()).isNotNull();
    }

    /**
     * Make sure we can get exactly midnight UTC.
     */
    @Test
    void canGetMidnightUtc() {
        final Instant midnightUTC = TaskUtils.getMidnightUTC();
        final ZonedDateTime cal = ZonedDateTime.ofInstant(midnightUTC, ZoneId.of("UTC"));
        Assertions.assertThat(cal.getNano()).isEqualTo(0);
        Assertions.assertThat(cal.getSecond()).isEqualTo(0);
        Assertions.assertThat(cal.getMinute()).isEqualTo(0);
        Assertions.assertThat(cal.getHour()).isEqualTo(0);
    }
}
