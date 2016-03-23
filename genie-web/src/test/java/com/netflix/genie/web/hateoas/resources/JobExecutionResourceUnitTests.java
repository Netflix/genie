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
package com.netflix.genie.web.hateoas.resources;

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Date;
import java.util.UUID;

/**
 * Unit tests for the JobExecutionResource class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobExecutionResourceUnitTests {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final int PID = 38091;
    private static final long CHECK_DELAY = 813309L;
    private static final Date TIMEOUT = new Date();

    private JobExecution jobExecution;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobExecution = new JobExecution
            .Builder(NAME, PID, CHECK_DELAY, TIMEOUT)
            .withId(ID)
            .build();
    }

    /**
     * Make sure we can build the resource.
     */
    @Test
    public void canBuildResource() {
        Assert.assertNotNull(new JobExecutionResource(this.jobExecution));
    }
}
