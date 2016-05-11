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

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.Date;

/**
 * Unit tests for TimeUtils.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class TimeUtilsUnitTests {

    /**
     * Just ignore the constructor.
     */
    @Test
    public void canConstruct() {
        Assert.assertNotNull(new TimeUtils());
    }

    /**
     * Can get the duration for various cases.
     */
    @Test
    public void canGetDuration() {
        final Date epoch = new Date(0);
        final long durationMillis = 50823L;
        final Duration duration = Duration.ofMillis(durationMillis);
        final Date started = new Date();
        final Date finished = new Date(started.getTime() + durationMillis);

        Assert.assertThat(TimeUtils.getDuration(null, finished), Matchers.is(Duration.ZERO));
        Assert.assertThat(TimeUtils.getDuration(epoch, finished), Matchers.is(Duration.ZERO));

        Assert.assertNotNull(TimeUtils.getDuration(started, null));
        Assert.assertNotNull(TimeUtils.getDuration(started, epoch));

        Assert.assertThat(TimeUtils.getDuration(started, finished), Matchers.is(duration));
    }
}
