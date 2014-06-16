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
package com.netflix.genie.common.model;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for Job Status utility methods.
 *
 * @author skrishnan
 */
public class TestJobStatus {

    /**
     * Tests whether a job status is updated correctly, and update time is
     * changed accordingly.
     */
    @Test
    public void testSetJobStatus() {
        Job job = new Job();

        // finish time is 0 on initialization
        Assert.assertTrue(0L == job.getFinishTime());

        // start time is not zero on INIT, finish time is still 0
        job.setJobStatus(Types.JobStatus.INIT);
        Assert.assertNotNull(job.getStartTime());
        Assert.assertTrue(0L == job.getFinishTime());

        // finish time is non-zero on completion
        job.setJobStatus(Types.JobStatus.SUCCEEDED);
        Assert.assertFalse(0L == job.getFinishTime());
    }
}
