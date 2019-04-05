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

import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit test for UserJobCount.
 *
 * @author mprimi
 * @since 3.3.19
 */
@Category(UnitTest.class)
public class UserJobCountUnitTest {

    /**
     * Test constructor, accessors, equality.
     */
    @Test
    public void testUserJobCount() {
        final UserJobCount dto = new UserJobCount("foo", 3);

        Assert.assertEquals(3, dto.getCount());
        Assert.assertEquals("foo", dto.getUser());

        Assert.assertEquals(dto, new UserJobCount(dto.getUser(), dto.getCount()));
        Assert.assertNotEquals(dto, new UserJobCount(dto.getUser() + "bar", dto.getCount()));
        Assert.assertNotEquals(dto, new UserJobCount(dto.getUser(), dto.getCount() + 1));
    }
}
