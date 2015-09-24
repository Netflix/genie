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
package com.netflix.genie.common.dto;

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the JobStatus enum.
 *
 * @author tgianos
 */
public class JobStatusTests {

    /**
     * Tests whether a valid job status is parsed correctly.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testValidJobStatus() throws GeniePreconditionException {
        Assert.assertEquals(JobStatus.RUNNING, JobStatus.parse(JobStatus.RUNNING.name().toLowerCase()));
        Assert.assertEquals(JobStatus.FAILED, JobStatus.parse(JobStatus.FAILED.name().toLowerCase()));
        Assert.assertEquals(JobStatus.KILLED, JobStatus.parse(JobStatus.KILLED.name().toLowerCase()));
        Assert.assertEquals(JobStatus.INIT, JobStatus.parse(JobStatus.INIT.name().toLowerCase()));
        Assert.assertEquals(JobStatus.SUCCEEDED, JobStatus.parse(JobStatus.SUCCEEDED.name().toLowerCase()));
    }

    /**
     * Tests whether an invalid job status returns null.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testInvalidJobStatus() throws GeniePreconditionException {
        JobStatus.parse("DOES_NOT_EXIST");
    }

    /**
     * Tests whether an invalid application status throws exception.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testBlankJobStatus() throws GeniePreconditionException {
        JobStatus.parse(null);
    }
}
