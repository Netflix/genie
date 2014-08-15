/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.services.impl.jpa;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.server.services.ExecutionService;
import java.net.HttpURLConnection;
import java.util.Calendar;
import java.util.UUID;
import javax.inject.Inject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for the Genie execution service class.
 *
 * @author skrishnan
 * @author tgianos
 */
@DatabaseSetup("execution/init.xml")
public class TestExecutionServiceJPAImpl extends DBUnitTestBase {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_2_ID = "job2";
    private static final String JOB_3_ID = "job3";
    private static final String JOB_4_ID = "job4";
    private static final String JOB_5_ID = "job5";
    private static final String JOB_6_ID = "job6";

    @Inject
    private ExecutionService xs;

    /**
     * Test submitting a null job.
     */
    @Test
    public void testSubmitJobNoJob() {
        try {
            this.xs.submitJob(null);
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_PRECON_FAILED,
                    ge.getErrorCode()
            );
        }
    }

    /**
     * Test submitting a job that already exists.
     */
    @Test
    public void testSubmitJobThatExists() {
        try {
            final Job job = new Job();
            job.setId(JOB_1_ID);
            this.xs.submitJob(job);
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_CONFLICT,
                    ge.getErrorCode()
            );
        }
    }

    /**
     * Test to make sure already failed/finished jobs don't get killed again.
     *
     * @throws GenieException if anything went wrong with the test.
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
     * Test whether a job kill returns immediately for a finished job.
     */
    @Test
    public void testTryingToKillInitializingJob() {
        try {
            this.xs.killJob(JOB_4_ID);
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_PRECON_FAILED,
                    ge.getErrorCode()
            );
        }
//        try {
//            this.xs.killJob(JOB_5_ID);
//            Assert.fail();
//        } catch (final GenieException ge) {
//            Assert.assertEquals(
//                    HttpURLConnection.HTTP_PRECON_FAILED,
//                    ge.getErrorCode()
//            );
//        }
    }

    /**
     * Test killing a job with no id passed in.
     */
    @Test
    public void testKillJobNoId() {
        try {
            this.xs.killJob(null);
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_PRECON_FAILED,
                    ge.getErrorCode()
            );
        }
    }

    /**
     * Test killing a job with no job exists.
     */
    @Test
    public void testKillJobNoJob() {
        try {
            this.xs.killJob(UUID.randomUUID().toString());
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    ge.getErrorCode()
            );
        }
    }

    /**
     * Test killing a job with no kill URI.
     */
    @Test
    public void testKillJobNoKillURI() {
        try {
            this.xs.killJob(JOB_6_ID);
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_PRECON_FAILED,
                    ge.getErrorCode()
            );
        }
    }
}
