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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

/**
 * Unit tests for TimeUtils.
 *
 * @author tgianos
 * @since 3.0.0
 */
class TimeUtilsTest {

    /**
     * Can get the duration for various cases.
     */
    @Test
    void canGetDuration() {
        final long durationMillis = 50823L;
        final Duration duration = Duration.ofMillis(durationMillis);
        final Instant started = Instant.now();
        final Instant finished = started.plusMillis(durationMillis);

        Assertions.assertThat(TimeUtils.getDuration(null, finished)).isEqualByComparingTo(Duration.ZERO);
        Assertions.assertThat(TimeUtils.getDuration(Instant.EPOCH, finished)).isEqualByComparingTo(Duration.ZERO);

        Assertions.assertThat(TimeUtils.getDuration(started, null)).isNotNull();
        Assertions.assertThat(TimeUtils.getDuration(started, Instant.EPOCH)).isNotNull();

        Assertions.assertThat(TimeUtils.getDuration(started, finished)).isEqualByComparingTo(duration);
    }
}
