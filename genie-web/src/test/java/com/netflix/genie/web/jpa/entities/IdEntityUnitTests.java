/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.jpa.entities;

import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit tests for the IdEntity class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(UnitTest.class)
public class IdEntityUnitTests {

    /**
     * Test the toString method.
     */
    @Test
    public void testToString() {
        Assert.assertNotNull(new IdEntity().toString());
    }
}
