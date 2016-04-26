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

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Calendar;

/**
 * Unit tests for the utility methods for task.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class TaskUtilsUnitTests {

    /**
     * Make sure that since we're in the same package we can construct.
     */
    @Test
    public void canConstruct() {
        Assert.assertNotNull(new TaskUtils());
    }

    /**
     * Make sure we can get exactly midnight UTC.
     */
    @Test
    public void canGetMidnightUtc() {
        final Calendar cal = TaskUtils.getMidnightUTC();
        Assert.assertThat(cal.get(Calendar.MILLISECOND), Matchers.is(0));
        Assert.assertThat(cal.get(Calendar.SECOND), Matchers.is(0));
        Assert.assertThat(cal.get(Calendar.MINUTE), Matchers.is(0));
        Assert.assertThat(cal.get(Calendar.HOUR_OF_DAY), Matchers.is(0));
    }

    /**
     * Make sure we can subtract the number of desired days from a calendar date properly.
     */
    @Test
    public void canSubtractDaysFromDate() {
        final int currentDay = 25;
        final Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DAY_OF_YEAR, currentDay);
        final int retention = 5;
        TaskUtils.subtractDaysFromDate(cal, retention);
        Assert.assertThat(cal.get(Calendar.DAY_OF_YEAR), Matchers.is(20));
        TaskUtils.subtractDaysFromDate(cal, -1 * retention);
        Assert.assertThat(cal.get(Calendar.DAY_OF_YEAR), Matchers.is(15));
    }
}
