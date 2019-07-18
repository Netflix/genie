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
package com.netflix.genie.web.spring.autoconfigure;

import com.netflix.genie.GenieTestApp;
import com.netflix.genie.web.properties.DataServiceRetryProperties;
import com.netflix.genie.web.properties.HealthProperties;
import com.netflix.genie.web.properties.JobsProperties;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration test for PropertiesConfig values binding.
 *
 * @author mprimi
 * @since 3.1.0
 */
@Ignore("This seems more like a test of Spring functionality than Genie functionality, do we need it?")
@RunWith(SpringRunner.class)
@TestPropertySource(locations = "classpath:/PropertyBindingIntegrationTests.properties")
@SpringBootTest(
    classes = GenieTestApp.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE
)
public class PropertyBindingIntegrationTest {

    @Autowired
    private JobsProperties jobsProperties;

    @Autowired
    private DataServiceRetryProperties dataServiceRetryProperties;

    @Autowired
    private HealthProperties healthProperties;

    /**
     * Verify than beans get autowired, and that (non-default) values correspond to the expected set via properties
     * file.
     */
    @Test
    public void testPropertiesValues() {
        Assert.assertNotNull(jobsProperties);
        Assert.assertThat(jobsProperties.getForwarding().isEnabled(), Matchers.is(true));
        Assert.assertThat(jobsProperties.getLocations().getJobs(), Matchers.is("file:///tmp"));
        Assert.assertThat(jobsProperties.getMax().getStdOutSize(), Matchers.is(512L));
        Assert.assertThat(jobsProperties.getMemory().getMaxSystemMemory(), Matchers.is(1024));
        Assert.assertThat(jobsProperties.getUsers().isCreationEnabled(), Matchers.is(true));
        Assert.assertThat(jobsProperties.getActiveLimit().isEnabled(), Matchers.is(true));
        Assert.assertThat(jobsProperties.getActiveLimit().getCount(), Matchers.is(15));
        Assert.assertThat(jobsProperties.getActiveLimit().getUserLimit("JaneDoe"), Matchers.is(100));
        Assert.assertThat(jobsProperties.getActiveLimit().getUserLimit("janedoe"), Matchers.is(100));
        Assert.assertThat(jobsProperties.getActiveLimit().getUserLimit("John-Doe"), Matchers.is(200));
        Assert.assertThat(jobsProperties.getActiveLimit().getUserLimit("john-doe"), Matchers.is(200));
        Assert.assertThat(jobsProperties.getActiveLimit().getUserLimit("anyone else"), Matchers.is(15));

        Assert.assertNotNull(dataServiceRetryProperties);
        Assert.assertThat(dataServiceRetryProperties.getInitialInterval(), Matchers.is(200L));

        Assert.assertNotNull(healthProperties);
        Assert.assertThat(healthProperties.getMaxCpuLoadPercent(), Matchers.is(33.3));
        Assert.assertThat(healthProperties.getMaxCpuLoadConsecutiveOccurrences(), Matchers.is(5));
    }
}
