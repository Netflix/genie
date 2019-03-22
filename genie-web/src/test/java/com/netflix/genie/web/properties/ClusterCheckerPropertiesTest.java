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

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests for ClusterCheckerProperties.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class ClusterCheckerPropertiesTest {

    private ClusterCheckerProperties properties;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.properties = new ClusterCheckerProperties();
    }

    /**
     * Make sure we get reasonable default properties.
     */
    @Test
    public void canConstructWithValidProperties() {
        Assert.assertThat(this.properties.getPort(), Matchers.is(8080));
        Assert.assertThat(this.properties.getScheme(), Matchers.is("http"));
        Assert.assertThat(this.properties.getLostThreshold(), Matchers.is(3));
        Assert.assertThat(this.properties.getRate(), Matchers.is(300000L));
    }

    /**
     * Make sure we can set the port.
     */
    @Test
    public void canSetPort() {
        final int port = 7001;
        this.properties.setPort(port);
        Assert.assertThat(this.properties.getPort(), Matchers.is(port));
    }

    /**
     * Make sure we can set the scheme.
     */
    @Test
    public void canSetScheme() {
        final String scheme = UUID.randomUUID().toString();
        this.properties.setScheme(scheme);
        Assert.assertThat(this.properties.getScheme(), Matchers.is(scheme));
    }

    /**
     * Make sure we can set the lost threshold.
     */
    @Test
    public void canSetLostThreshold() {
        final int lostThreshold = 89;
        this.properties.setLostThreshold(lostThreshold);
        Assert.assertThat(this.properties.getLostThreshold(), Matchers.is(lostThreshold));
    }

    /**
     * Make sure we can set the check rate.
     */
    @Test
    public void canSetRate() {
        final long rate = 808283L;
        this.properties.setRate(rate);
        Assert.assertThat(this.properties.getRate(), Matchers.is(rate));
    }
}
