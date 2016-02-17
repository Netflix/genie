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
package com.netflix.genie.web.tasks.leader;

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit tests for the ZombieTask class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class ZombieTaskUnitTests {

    private ZombieTask task;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.task = new ZombieTask();
    }

    /**
     * Make sure run method works.
     */
    @Test
    public void canRun() {
        // TODO: flesh out once this is implemented
        this.task.run();
    }

    /**
     * Make sure the trigger is null.
     */
    @Test
    public void canGetTrigger() {
        Assert.assertNull(this.task.getTrigger());
    }

    /**
     * Make sure the get period returns the correct value.
     */
    @Test
    public void canGetPeriod() {
        // TODO: flesh out
        Assert.assertThat(this.task.getPeriod(), Matchers.is(45000L));
    }
}
