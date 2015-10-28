/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.jpa.services;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.ExecutionService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.validation.ConstraintViolationException;
import java.util.Calendar;

/**
 * Test for the Genie execution service class.
 *
 * @author tgianos
 */
@DatabaseSetup("ExecutionServiceJPAImplIntegrationTests/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaExecutionServiceImplIntegrationTests extends DBUnitTestBase {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_2_ID = "job2";
    private static final String JOB_3_ID = "job3";

    @Autowired
    private ExecutionService xs;

    /**
     * Test submitting a null job.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testSubmitJobNoJob() throws GenieException {
        this.xs.submitJob(null);
    }

    /**
     * Test to make sure already failed/finished jobs don't get killed again.
     *
     * @throws GenieException For any problem if anything went wrong with the test.
     */
    @Test
    public void testKillJob() throws GenieException {
        final Calendar one = Calendar.getInstance();
        one.clear();
        one.set(2014, Calendar.JANUARY, 2, 1, 50, 0);
        final Calendar two = Calendar.getInstance();
        two.clear();
        two.set(2014, Calendar.JANUARY, 3, 1, 50, 0);
        final Calendar three = Calendar.getInstance();
        three.clear();
        three.set(2014, Calendar.JANUARY, 4, 1, 50, 0);
        // should return immediately despite bogus killURI
        final Job job1 = this.xs.killJob(JOB_1_ID);
        Assert.assertEquals(JobStatus.SUCCEEDED, job1.getStatus());
        Assert.assertEquals(one.getTimeInMillis(), job1.getUpdated().getTime());
        final Job job2 = this.xs.killJob(JOB_2_ID);
        Assert.assertEquals(JobStatus.KILLED, job2.getStatus());
        Assert.assertEquals(two.getTimeInMillis(), job2.getUpdated().getTime());
        final Job job3 = this.xs.killJob(JOB_3_ID);
        Assert.assertEquals(JobStatus.FAILED, job3.getStatus());
        Assert.assertEquals(three.getTimeInMillis(), job3.getUpdated().getTime());
    }

    /**
     * Test killing a job with no id passed in.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testKillJobNoId() throws GenieException {
        this.xs.killJob(null);
    }
}
