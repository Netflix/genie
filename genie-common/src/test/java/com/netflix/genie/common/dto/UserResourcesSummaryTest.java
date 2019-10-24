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
package com.netflix.genie.common.dto;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test UserResourcesSummary DTO.
 */
class UserResourcesSummaryTest {

    /**
     * Test constructor, accessors, equality.
     */
    @Test
    void testUserJobCount() {
        final UserResourcesSummary dto = new UserResourcesSummary("foo", 3, 1024);

        Assertions.assertThat(dto.getUser()).isEqualTo("foo");
        Assertions.assertThat(dto.getRunningJobsCount()).isEqualTo(3);
        Assertions.assertThat(dto.getUsedMemory()).isEqualTo(1024);

        Assertions.assertThat(dto).isEqualTo(new UserResourcesSummary("foo", 3, 1024));
        Assertions.assertThat(dto).isNotEqualTo(new UserResourcesSummary("bar", 3, 1024));
        Assertions.assertThat(dto).isNotEqualTo(new UserResourcesSummary("foo", 4, 1024));
        Assertions.assertThat(dto).isNotEqualTo(new UserResourcesSummary("foo", 3, 2048));


        Assertions.assertThat(dto.hashCode()).isEqualTo(new UserResourcesSummary("foo", 3, 1024).hashCode());
        Assertions.assertThat(dto.hashCode()).isNotEqualTo(new UserResourcesSummary("bar", 3, 1024).hashCode());
        Assertions.assertThat(dto.hashCode()).isNotEqualTo(new UserResourcesSummary("foo", 4, 1024).hashCode());
        Assertions.assertThat(dto.hashCode()).isNotEqualTo(new UserResourcesSummary("foo", 3, 2048).hashCode());
    }
}
