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

import com.netflix.genie.common.exceptions.CloudServiceException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for utility methods under Types.
 *
 * @author skrishnan
 * @author tgianos
 */
public class TestTypes {

    /**
     * Tests whether a valid cluster status is parsed correctly.
     *
     * @throws com.netflix.genie.common.exceptions.CloudServiceException
     */
    @Test
    public void testValidClusterStatus() throws CloudServiceException {
        String status = ClusterStatus.UP.name();
        Assert.assertEquals(ClusterStatus.UP, ClusterStatus.parse(status));
    }

    /**
     * Tests whether an invalid cluster status returns null.
     *
     * @throws com.netflix.genie.common.exceptions.CloudServiceException
     */
    @Test(expected = CloudServiceException.class)
    public void testInvalidClusterStatus() throws CloudServiceException {
        ClusterStatus.parse("DOES_NOT_EXIST");
    }

    /**
     * Tests whether a valid job status is parsed correctly.
     *
     * @throws com.netflix.genie.common.exceptions.CloudServiceException
     */
    @Test
    public void testValidJobStatus() throws CloudServiceException {
        String status = JobStatus.RUNNING.name();
        Assert.assertEquals(JobStatus.RUNNING, JobStatus.parse(status));
    }

    /**
     * Tests whether an invalid job status returns null.
     *
     * @throws com.netflix.genie.common.exceptions.CloudServiceException
     */
    @Test(expected = CloudServiceException.class)
    public void testInvalidJobStatus() throws CloudServiceException {
        JobStatus.parse("DOES_NOT_EXIST");
    }
}
