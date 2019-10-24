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
 * Tests for the ApplicationStatus enum.
 *
 * @author tgianos
 * @since 2.0.0
 */
class ApplicationStatusTest {

    /**
     * Tests whether a valid application status is parsed correctly.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    void testValidApplicationStatus() throws GeniePreconditionException {
        Assertions
            .assertThat(ApplicationStatus.parse(ApplicationStatus.ACTIVE.name().toLowerCase()))
            .isEqualByComparingTo(ApplicationStatus.ACTIVE);
        Assertions
            .assertThat(ApplicationStatus.parse(ApplicationStatus.DEPRECATED.name().toLowerCase()))
            .isEqualByComparingTo(ApplicationStatus.DEPRECATED);
        Assertions
            .assertThat(ApplicationStatus.parse(ApplicationStatus.INACTIVE.name().toLowerCase()))
            .isEqualByComparingTo(ApplicationStatus.INACTIVE);
    }

    /**
     * Tests whether an invalid application status throws exception.
     */
    @Test
    void testInvalidApplicationStatus() {
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> ApplicationStatus.parse("DOES_NOT_EXIST"));
    }

    /**
     * Tests whether an invalid application status throws exception.
     */
    @Test
    void testBlankApplicationStatus() {
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> ApplicationStatus.parse(" "));
    }
}
