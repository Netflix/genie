/*
 *
 *  Copyright 2013 Netflix, Inc.
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
 * Test case for utility methods under Types.
 *
 * @author skrishnan
 */
public class TestTypes {

    /**
     * Tests whether a valid cluster status is parsed correctly.
     */
    @Test
    public void testValidClusterStatus() {
        String status = Types.ClusterStatus.UP.name();
        Assert.assertEquals(Types.ClusterStatus.parse(status),
                Types.ClusterStatus.UP);
    }

    /**
     * Tests whether an invalid cluster status returns null.
     */
    @Test
    public void testInvalidClusterStatus() {
        String status = "DOES_NOT_EXIST";
        Assert.assertNull(Types.ClusterStatus.parse(status));
    }

    /**
     * Tests whether a valid configuration is parsed correctly.
     */
    @Test
    public void testValidConfiguration() {
        String config = Types.Configuration.PROD.name();
        Assert.assertEquals(Types.Configuration.parse(config),
                Types.Configuration.PROD);
    }

    /**
     * Tests whether an invalid configuration returns null.
     */
    @Test
    public void testInvalidConfiguration() {
        String config = "DOES_NOT_EXIST";
        Assert.assertNull(Types.Configuration.parse(config));
    }

    /**
     * Tests whether a valid job status is parsed correctly.
     */
    @Test
    public void testValidJobStatus() {
        String status = Types.JobStatus.RUNNING.name();
        Assert.assertEquals(Types.JobStatus.parse(status),
                Types.JobStatus.RUNNING);
    }

    /**
     * Tests whether an invalid job status returns null.
     */
    @Test
    public void testInvalidJobStatus() {
        String status = "DOES_NOT_EXIST";
        Assert.assertNull(Types.JobStatus.parse(status));
    }

    /**
     * Tests whether a valid schedule is parsed correctly.
     */
    @Test
    public void testValidSchedule() {
        String schedule = Types.Schedule.ADHOC.name();
        Assert.assertEquals(Types.Schedule.parse(schedule),
                Types.Schedule.ADHOC);
    }

    /**
     * Tests whether an invalid schedule returns null.
     */
    @Test
    public void testInvalidSchedule() {
        String schedule = "DOES_NOT_EXIST";
        Assert.assertNull(Types.Schedule.parse(schedule));
    }

    /**
     * Tests whether a valid job type is parsed correctly.
     */
    @Test
    public void testValidJobType() {
        String type = Types.JobType.HIVE.name();
        Assert.assertEquals(Types.JobType.parse(type), Types.JobType.HIVE);
    }

    /**
     * Tests whether an invalid job type returns null.
     */
    @Test
    public void testInvalidJobType() {
        String type = "DOES_NOT_EXIST";
        Assert.assertNull(Types.JobType.parse(type));
    }
}
