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
package com.netflix.genie.security.configs;

import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit tests for HttpSessionConfig.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class HttpSessionConfigUnitTests {

    private HttpSessionConfig httpSessionConfig;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.httpSessionConfig = new HttpSessionConfig();
    }

    /**
     * Make sure we can construct the config class.
     */
    @Test
    public void canConstruct() {
        Assert.assertNotNull(this.httpSessionConfig);
    }

    /**
     * Make sure the post construct method is called.
     */
    @Test
    public void canPostConstruct() {
        this.httpSessionConfig.postConstruct();
    }
}
