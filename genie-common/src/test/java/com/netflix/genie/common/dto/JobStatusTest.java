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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for the JobStatus enum.
 *
 * @author tgianos
 * @since 2.0.0
 */
class JobStatusTest {

    /**
     * Tests whether a valid job status is parsed correctly.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    void testValidJobStatus() throws GeniePreconditionException {
        Assertions
            .assertThat(JobStatus.parse(JobStatus.RUNNING.name().toLowerCase()))
            .isEqualByComparingTo(JobStatus.RUNNING);
        Assertions
            .assertThat(JobStatus.parse(JobStatus.FAILED.name().toLowerCase()))
            .isEqualByComparingTo(JobStatus.FAILED);
        Assertions
            .assertThat(JobStatus.parse(JobStatus.KILLED.name().toLowerCase()))
            .isEqualByComparingTo(JobStatus.KILLED);
        Assertions
            .assertThat(JobStatus.parse(JobStatus.INIT.name().toLowerCase()))
            .isEqualByComparingTo(JobStatus.INIT);
        Assertions
            .assertThat(JobStatus.parse(JobStatus.SUCCEEDED.name().toLowerCase()))
            .isEqualByComparingTo(JobStatus.SUCCEEDED);
        Assertions
            .assertThat(JobStatus.parse(JobStatus.RESERVED.name().toLowerCase()))
            .isEqualByComparingTo(JobStatus.RESERVED);
        Assertions
            .assertThat(JobStatus.parse(JobStatus.RESOLVED.name().toLowerCase()))
            .isEqualByComparingTo(JobStatus.RESOLVED);
        Assertions
            .assertThat(JobStatus.parse(JobStatus.CLAIMED.name().toLowerCase()))
            .isEqualByComparingTo(JobStatus.CLAIMED);
        Assertions
            .assertThat(JobStatus.parse(JobStatus.ACCEPTED.name().toLowerCase()))
            .isEqualByComparingTo(JobStatus.ACCEPTED);
    }

    /**
     * Tests whether an invalid job status returns null.
     */
    @Test
    void testInvalidJobStatus() {
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> JobStatus.parse("DOES_NOT_EXIST"));
    }

    /**
     * Tests whether an invalid application status throws exception.
     */
    @Test
    void testBlankJobStatus() {
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> JobStatus.parse("  "));
    }

    /**
     * Test to make sure isActive is working properly.
     */
    @Test
    void testIsActive() {
        Assertions.assertThat(JobStatus.RUNNING.isActive()).isTrue();
        Assertions.assertThat(JobStatus.INIT.isActive()).isTrue();
        Assertions.assertThat(JobStatus.FAILED.isActive()).isFalse();
        Assertions.assertThat(JobStatus.INVALID.isActive()).isFalse();
        Assertions.assertThat(JobStatus.KILLED.isActive()).isFalse();
        Assertions.assertThat(JobStatus.SUCCEEDED.isActive()).isFalse();
        Assertions.assertThat(JobStatus.RESERVED.isActive()).isTrue();
        Assertions.assertThat(JobStatus.RESOLVED.isActive()).isTrue();
        Assertions.assertThat(JobStatus.CLAIMED.isActive()).isTrue();
        Assertions.assertThat(JobStatus.ACCEPTED.isActive()).isTrue();
    }

    /**
     * Test to make sure isFinished is working properly.
     */
    @Test
    void testIsFinished() {
        Assertions.assertThat(JobStatus.RUNNING.isFinished()).isFalse();
        Assertions.assertThat(JobStatus.INIT.isFinished()).isFalse();
        Assertions.assertThat(JobStatus.FAILED.isFinished()).isTrue();
        Assertions.assertThat(JobStatus.INVALID.isFinished()).isTrue();
        Assertions.assertThat(JobStatus.KILLED.isFinished()).isTrue();
        Assertions.assertThat(JobStatus.SUCCEEDED.isFinished()).isTrue();
        Assertions.assertThat(JobStatus.RESERVED.isFinished()).isFalse();
        Assertions.assertThat(JobStatus.RESOLVED.isFinished()).isFalse();
        Assertions.assertThat(JobStatus.CLAIMED.isFinished()).isFalse();
        Assertions.assertThat(JobStatus.ACCEPTED.isFinished()).isFalse();
    }

    /**
     * Test to make sure isResolvable is working properly.
     */
    @Test
    void testIsResolvable() {
        Assertions.assertThat(JobStatus.RUNNING.isResolvable()).isFalse();
        Assertions.assertThat(JobStatus.INIT.isResolvable()).isFalse();
        Assertions.assertThat(JobStatus.FAILED.isResolvable()).isFalse();
        Assertions.assertThat(JobStatus.INVALID.isResolvable()).isFalse();
        Assertions.assertThat(JobStatus.KILLED.isResolvable()).isFalse();
        Assertions.assertThat(JobStatus.SUCCEEDED.isResolvable()).isFalse();
        Assertions.assertThat(JobStatus.RESERVED.isResolvable()).isTrue();
        Assertions.assertThat(JobStatus.RESOLVED.isResolvable()).isFalse();
        Assertions.assertThat(JobStatus.CLAIMED.isResolvable()).isFalse();
        Assertions.assertThat(JobStatus.ACCEPTED.isResolvable()).isFalse();
    }

    /**
     * Test to make sure isClaimable is working properly.
     */
    @Test
    void testIsClaimable() {
        Assertions.assertThat(JobStatus.RUNNING.isClaimable()).isFalse();
        Assertions.assertThat(JobStatus.INIT.isClaimable()).isFalse();
        Assertions.assertThat(JobStatus.FAILED.isClaimable()).isFalse();
        Assertions.assertThat(JobStatus.INVALID.isClaimable()).isFalse();
        Assertions.assertThat(JobStatus.KILLED.isClaimable()).isFalse();
        Assertions.assertThat(JobStatus.SUCCEEDED.isClaimable()).isFalse();
        Assertions.assertThat(JobStatus.RESERVED.isClaimable()).isFalse();
        Assertions.assertThat(JobStatus.RESOLVED.isClaimable()).isTrue();
        Assertions.assertThat(JobStatus.CLAIMED.isClaimable()).isFalse();
        Assertions.assertThat(JobStatus.ACCEPTED.isClaimable()).isTrue();
    }

    /**
     * Make sure all the active statuses are present in the set.
     */
    @Test
    void testGetActivesStatuses() {
        Assertions
            .assertThat(JobStatus.getActiveStatuses())
            .containsExactlyInAnyOrder(
                JobStatus.INIT,
                JobStatus.RUNNING,
                JobStatus.RESERVED,
                JobStatus.RESOLVED,
                JobStatus.CLAIMED,
                JobStatus.ACCEPTED
            );
    }

    /**
     * Make sure all the finished statuses are present in the set.
     */
    @Test
    void testGetFinishedStatuses() {
        Assertions
            .assertThat(JobStatus.getFinishedStatuses())
            .containsExactlyInAnyOrder(JobStatus.INVALID, JobStatus.FAILED, JobStatus.KILLED, JobStatus.SUCCEEDED);
    }

    /**
     * Make sure all the claimable status are present in the set.
     */
    @Test
    void testGetResolvableStatuses() {
        Assertions.assertThat(JobStatus.getResolvableStatuses()).containsExactlyInAnyOrder(JobStatus.RESERVED);
    }

    /**
     * Make sure all the claimable status are present in the set.
     */
    @Test
    void testGetClaimableStatuses() {
        Assertions
            .assertThat(JobStatus.getClaimableStatuses())
            .containsExactlyInAnyOrder(JobStatus.RESOLVED, JobStatus.ACCEPTED);
    }
}
