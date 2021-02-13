/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.common.external.dtos.v4;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JobStatus}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class JobStatusTest {

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

    @Test
    void testGetFinishedStatuses() {
        Assertions
            .assertThat(JobStatus.getFinishedStatuses())
            .containsExactlyInAnyOrder(JobStatus.INVALID, JobStatus.FAILED, JobStatus.KILLED, JobStatus.SUCCEEDED);
    }

    @Test
    void testGetResolvableStatuses() {
        Assertions.assertThat(JobStatus.getResolvableStatuses()).containsExactlyInAnyOrder(JobStatus.RESERVED);
    }

    @Test
    void testGetClaimableStatuses() {
        Assertions
            .assertThat(JobStatus.getClaimableStatuses())
            .containsExactlyInAnyOrder(JobStatus.RESOLVED, JobStatus.ACCEPTED);
    }

    @Test
    void testGetStatusesBeforeClaimed() {
        Assertions
            .assertThat(JobStatus.getStatusesBeforeClaimed())
            .containsExactlyInAnyOrder(JobStatus.ACCEPTED, JobStatus.RESERVED, JobStatus.RESOLVED);
    }
}
