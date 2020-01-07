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

import com.netflix.genie.common.external.util.GenieObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

/**
 * Unit tests for the BaseDTO class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class BaseDTOTest {

    /**
     * Test to make sure we can create a valid JSON string from a DTO object.
     */
    @Test
    void canCreateValidJsonString() {
        final Application application = new Application.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            ApplicationStatus.ACTIVE
        ).build();

        final String json = application.toString();
        Assertions.assertThatCode(() -> GenieObjectMapper.getMapper().readTree(json)).doesNotThrowAnyException();
    }
}
