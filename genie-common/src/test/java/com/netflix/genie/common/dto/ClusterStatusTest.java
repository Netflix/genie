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
 * Tests for the ClusterStatus enum.
 *
 * @author tgianos
 * @since 2.0.0
 */
class ClusterStatusTest {

    /**
     * Tests whether a valid cluster status is parsed correctly.
     *
     * @throws GeniePreconditionException if any precondition isn't met.
     */
    @Test
    void testValidClusterStatus() throws GeniePreconditionException {
        Assertions
            .assertThat(ClusterStatus.parse(ClusterStatus.UP.name().toLowerCase()))
            .isEqualByComparingTo(ClusterStatus.UP);
        Assertions
            .assertThat(ClusterStatus.parse(ClusterStatus.OUT_OF_SERVICE.name().toLowerCase()))
            .isEqualByComparingTo(ClusterStatus.OUT_OF_SERVICE);
        Assertions
            .assertThat(ClusterStatus.parse(ClusterStatus.TERMINATED.name().toLowerCase()))
            .isEqualByComparingTo(ClusterStatus.TERMINATED);
    }

    /**
     * Tests whether an invalid cluster status throws exception.
     */
    @Test
    void testInvalidClusterStatus() {
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> ClusterStatus.parse("DOES_NOT_EXIST"));
    }

    /**
     * Tests whether an invalid cluster status throws exception.
     */
    @Test
    void testBlankClusterStatus() {
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> ClusterStatus.parse("  "));
    }
}
