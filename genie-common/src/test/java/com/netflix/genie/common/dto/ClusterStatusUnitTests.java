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
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for the ClusterStatus enum.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(UnitTest.class)
public class ClusterStatusUnitTests {

    /**
     * Tests whether a valid cluster status is parsed correctly.
     *
     * @throws GeniePreconditionException if any precondition isn't met.
     */
    @Test
    public void testValidClusterStatus() throws GeniePreconditionException {
        Assert.assertEquals(ClusterStatus.UP,
            ClusterStatus.parse(ClusterStatus.UP.name().toLowerCase()));
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE,
            ClusterStatus.parse(ClusterStatus.OUT_OF_SERVICE.name().toLowerCase()));
        Assert.assertEquals(ClusterStatus.TERMINATED,
            ClusterStatus.parse(ClusterStatus.TERMINATED.name().toLowerCase()));
    }

    /**
     * Tests whether an invalid cluster status throws exception.
     *
     * @throws GeniePreconditionException if any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testInvalidClusterStatus() throws GeniePreconditionException {
        ClusterStatus.parse("DOES_NOT_EXIST");
    }

    /**
     * Tests whether an invalid cluster status throws exception.
     *
     * @throws GeniePreconditionException if any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testBlankClusterStatus() throws GeniePreconditionException {
        ClusterStatus.parse("  ");
    }
}
