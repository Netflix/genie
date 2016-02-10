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
package com.netflix.genie;

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

/**
 * Unit tests for the GenieWeb class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieWebUnitTests {

    /**
     * Make sure we can construct a new Genie Web instance.
     */
    @Test
    public void canConstruct() {
        Assert.assertNotNull(new GenieWeb());
    }

    /**
     * Make sure we get the right default properties.
     */
    @Test
    public void canGetDefaultProperties() {
        final Map<String, Object> props = GenieWeb.getDefaultProperties();
        Assert.assertNotNull(props);
        Assert.assertThat(props.size(), Matchers.is(1));
        Assert.assertTrue(props.containsKey(GenieWeb.SPRING_CONFIG_LOCATION));
        Assert.assertThat(props.get(GenieWeb.SPRING_CONFIG_LOCATION), Matchers.is(GenieWeb.USER_HOME_GENIE));
    }
}
