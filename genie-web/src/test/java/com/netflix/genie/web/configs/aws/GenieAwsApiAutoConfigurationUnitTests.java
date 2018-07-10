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
package com.netflix.genie.web.configs.aws;

import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.UnknownHostException;

/**
 * Unit tests for the {@link GenieAwsApiAutoConfiguration} class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieAwsApiAutoConfigurationUnitTests {

    private GenieAwsApiAutoConfiguration genieAwsApiAutoConfiguration;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.genieAwsApiAutoConfiguration = new GenieAwsApiAutoConfiguration();
    }

    /**
     * Make sure we can get the {@link GenieHostInfo} instance even if EC2 metadata fails.
     *
     * @throws UnknownHostException When the fallback fails
     */
    @Test
    public void canGetGenieHostInfo() throws UnknownHostException {
        final GenieHostInfo genieHostInfo = this.genieAwsApiAutoConfiguration.genieHostInfo();
        Assert.assertThat(genieHostInfo.getHostname(), Matchers.notNullValue());
    }
}
