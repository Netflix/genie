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
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for the JobStatus enum.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(UnitTest.class)
public class JobStatusUnitTests {

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
        Assert.assertEquals(JobStatus.RESERVED, JobStatus.parse(JobStatus.RESERVED.name().toLowerCase()));
        Assert.assertEquals(JobStatus.RESOLVED, JobStatus.parse(JobStatus.RESOLVED.name().toLowerCase()));
        Assert.assertEquals(JobStatus.CLAIMED, JobStatus.parse(JobStatus.CLAIMED.name().toLowerCase()));
        Assert.assertEquals(JobStatus.ACCEPTED, JobStatus.parse(JobStatus.ACCEPTED.name().toLowerCase()));
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
        JobStatus.parse("  ");
    }

    /**
     * Test to make sure isActive is working properly.
     */
    @Test
    public void testIsActive() {
        Assert.assertTrue(JobStatus.RUNNING.isActive());
        Assert.assertTrue(JobStatus.INIT.isActive());
        Assert.assertFalse(JobStatus.FAILED.isActive());
        Assert.assertFalse(JobStatus.INVALID.isActive());
        Assert.assertFalse(JobStatus.KILLED.isActive());
        Assert.assertFalse(JobStatus.SUCCEEDED.isActive());
        Assert.assertTrue(JobStatus.RESERVED.isActive());
        Assert.assertTrue(JobStatus.RESOLVED.isActive());
        Assert.assertTrue(JobStatus.CLAIMED.isActive());
        Assert.assertTrue(JobStatus.ACCEPTED.isActive());
    }

    /**
     * Test to make sure isFinished is working properly.
     */
    @Test
    public void testIsFinished() {
        Assert.assertFalse(JobStatus.RUNNING.isFinished());
        Assert.assertFalse(JobStatus.INIT.isFinished());
        Assert.assertTrue(JobStatus.FAILED.isFinished());
        Assert.assertTrue(JobStatus.INVALID.isFinished());
        Assert.assertTrue(JobStatus.KILLED.isFinished());
        Assert.assertTrue(JobStatus.SUCCEEDED.isFinished());
        Assert.assertFalse(JobStatus.RESERVED.isFinished());
        Assert.assertFalse(JobStatus.RESOLVED.isFinished());
        Assert.assertFalse(JobStatus.CLAIMED.isFinished());
        Assert.assertFalse(JobStatus.ACCEPTED.isFinished());
    }

    /**
     * Test to make sure isResolvable is working properly.
     */
    @Test
    public void testIsResolvable() {
        Assert.assertFalse(JobStatus.RUNNING.isResolvable());
        Assert.assertFalse(JobStatus.INIT.isResolvable());
        Assert.assertFalse(JobStatus.FAILED.isResolvable());
        Assert.assertFalse(JobStatus.INVALID.isResolvable());
        Assert.assertFalse(JobStatus.KILLED.isResolvable());
        Assert.assertFalse(JobStatus.SUCCEEDED.isResolvable());
        Assert.assertTrue(JobStatus.RESERVED.isResolvable());
        Assert.assertFalse(JobStatus.RESOLVED.isResolvable());
        Assert.assertFalse(JobStatus.CLAIMED.isResolvable());
        Assert.assertFalse(JobStatus.ACCEPTED.isResolvable());
    }

    /**
     * Test to make sure isClaimable is working properly.
     */
    @Test
    public void testIsClaimable() {
        Assert.assertFalse(JobStatus.RUNNING.isClaimable());
        Assert.assertFalse(JobStatus.INIT.isClaimable());
        Assert.assertFalse(JobStatus.FAILED.isClaimable());
        Assert.assertFalse(JobStatus.INVALID.isClaimable());
        Assert.assertFalse(JobStatus.KILLED.isClaimable());
        Assert.assertFalse(JobStatus.SUCCEEDED.isClaimable());
        Assert.assertFalse(JobStatus.RESERVED.isClaimable());
        Assert.assertTrue(JobStatus.RESOLVED.isClaimable());
        Assert.assertFalse(JobStatus.CLAIMED.isClaimable());
        Assert.assertTrue(JobStatus.ACCEPTED.isClaimable());
    }

    /**
     * Make sure all the active statuses are present in the set.
     */
    @Test
    public void testGetActivesStatuses() {
        Assert.assertThat(JobStatus.getActiveStatuses().size(), Matchers.is(6));
        Assert.assertTrue(JobStatus.getActiveStatuses().contains(JobStatus.INIT));
        Assert.assertTrue(JobStatus.getActiveStatuses().contains(JobStatus.RUNNING));
        Assert.assertTrue(JobStatus.getActiveStatuses().contains(JobStatus.RESERVED));
        Assert.assertTrue(JobStatus.getActiveStatuses().contains(JobStatus.RESOLVED));
        Assert.assertTrue(JobStatus.getActiveStatuses().contains(JobStatus.CLAIMED));
        Assert.assertTrue(JobStatus.getActiveStatuses().contains(JobStatus.ACCEPTED));
    }

    /**
     * Make sure all the finished statuses are present in the set.
     */
    @Test
    public void testGetFinishedStatuses() {
        Assert.assertThat(JobStatus.getFinishedStatuses().size(), Matchers.is(4));
        Assert.assertTrue(JobStatus.getFinishedStatuses().contains(JobStatus.INVALID));
        Assert.assertTrue(JobStatus.getFinishedStatuses().contains(JobStatus.FAILED));
        Assert.assertTrue(JobStatus.getFinishedStatuses().contains(JobStatus.KILLED));
        Assert.assertTrue(JobStatus.getFinishedStatuses().contains(JobStatus.SUCCEEDED));
    }

    /**
     * Make sure all the claimable status are present in the set.
     */
    @Test
    public void testGetResolvableStatuses() {
        Assert.assertThat(JobStatus.getResolvableStatuses().size(), Matchers.is(1));
        Assert.assertThat(JobStatus.getResolvableStatuses(), Matchers.hasItem(JobStatus.RESERVED));
    }

    /**
     * Make sure all the claimable status are present in the set.
     */
    @Test
    public void testGetClaimableStatuses() {
        Assert.assertThat(JobStatus.getClaimableStatuses().size(), Matchers.is(2));
        Assert.assertThat(JobStatus.getClaimableStatuses(), Matchers.hasItems(JobStatus.RESOLVED, JobStatus.ACCEPTED));
    }
}
