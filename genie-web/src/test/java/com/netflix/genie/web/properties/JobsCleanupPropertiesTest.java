/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the cleanup properties.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JobsCleanupPropertiesTest {

    private JobsCleanupProperties properties;

    /**
     * Setup for tests.
     */
    @Before
    public void setup() {
        this.properties = new JobsCleanupProperties();
    }

    /**
     * Make sure properties are true by default.
     */
    @Test
    public void canConstruct() {
        Assert.assertTrue(this.properties.isDeleteDependencies());
    }

    /**
     * Make sure can set whether to delete the delete the dependencies.
     */
    @Test
    public void canSetDeleteDependencies() {
        this.properties.setDeleteDependencies(false);
        Assert.assertFalse(this.properties.isDeleteDependencies());
    }
}
