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

import org.junit.Assert;
import org.junit.Test;

/**
 * Test UserResourcesSummary DTO.
 */
public class UserResourcesSummaryTest {

    /**
     * Test constructor, accessors, equality.
     */
    @Test
    public void testUserJobCount() {
        final UserResourcesSummary dto = new UserResourcesSummary("foo", 3, 1024);

        Assert.assertEquals("foo", dto.getUser());
        Assert.assertEquals(3, dto.getRunningJobsCount());
        Assert.assertEquals(1024, dto.getUsedMemory());

        Assert.assertEquals(dto, new UserResourcesSummary("foo", 3, 1024));
        Assert.assertNotEquals(dto, new UserResourcesSummary("bar", 3, 1024));
        Assert.assertNotEquals(dto, new UserResourcesSummary("foo", 4, 1024));
        Assert.assertNotEquals(dto, new UserResourcesSummary("foo", 3, 2048));
    }
}
