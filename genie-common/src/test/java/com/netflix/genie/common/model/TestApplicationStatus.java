/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.GenieException;
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
     * @throws GenieException
     */
    @Test
    public void testValidApplicationStatus() throws GenieException {
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
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testInvalidApplicationStatus() throws GenieException {
        ApplicationStatus.parse("DOES_NOT_EXIST");
    }

    /**
     * Tests whether an invalid application status throws exception.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testBlankApplicationStatus() throws GenieException {
        ApplicationStatus.parse(null);
    }
}
