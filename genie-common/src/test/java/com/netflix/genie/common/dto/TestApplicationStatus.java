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
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the ApplicationStatus enum.
 *
 * @author tgianos
 */
public class TestApplicationStatus {

    /**
     * Tests whether a valid application status is parsed correctly.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testValidApplicationStatus() throws GeniePreconditionException {
        Assert.assertEquals(ApplicationStatus.ACTIVE,
                ApplicationStatus.parse(ApplicationStatus.ACTIVE.name().toLowerCase()));
        Assert.assertEquals(ApplicationStatus.DEPRECATED,
                ApplicationStatus.parse(ApplicationStatus.DEPRECATED.name().toLowerCase()));
        Assert.assertEquals(ApplicationStatus.INACTIVE,
                ApplicationStatus.parse(ApplicationStatus.INACTIVE.name().toLowerCase()));
    }

    /**
     * Tests whether an invalid application status throws exception.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testInvalidApplicationStatus() throws GeniePreconditionException {
        ApplicationStatus.parse("DOES_NOT_EXIST");
    }

    /**
     * Tests whether an invalid application status throws exception.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testBlankApplicationStatus() throws GeniePreconditionException {
        ApplicationStatus.parse(null);
    }
}
