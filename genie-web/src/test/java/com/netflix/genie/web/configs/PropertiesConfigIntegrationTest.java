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
package com.netflix.genie.web.configs;

import com.netflix.genie.core.properties.DataServiceRetryProperties;
import com.netflix.genie.core.properties.HealthProperties;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.properties.JobsUsersActiveLimitProperties;
import com.netflix.genie.test.categories.IntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@TestPropertySource(locations = "classpath:/PropertiesConfigIntegrationTest.properties")
@SpringBootTest()
public class PropertiesConfigIntegrationTest {

    @Autowired
    private JobsProperties jobsProperties;

    @Autowired
    private DataServiceRetryProperties dataServiceRetryProperties;

    @Autowired
    private HealthProperties healthProperties;

    @Autowired
    private JobsUsersActiveLimitProperties jobsUsersActiveLimitProperties;

    /**
     * Verify than beans get autowired, and that (non-default) values correspond to the expected set via properties
     * file.
     */
    @Test
    public void testPropertiesValues() {

        Assert.assertNotNull(jobsProperties);
        Assert.assertThat(jobsProperties.getCleanup().isDeleteArchiveFile(), Matchers.is(false));
        Assert.assertThat(jobsProperties.getForwarding().isEnabled(), Matchers.is(true));
        Assert.assertThat(jobsProperties.getLocations().getJobs(), Matchers.is("file:///tmp"));
        Assert.assertThat(jobsProperties.getMax().getStdOutSize(), Matchers.is(512L));
        Assert.assertThat(jobsProperties.getMemory().getMaxSystemMemory(), Matchers.is(1024));
        Assert.assertThat(jobsProperties.getUsers().isCreationEnabled(), Matchers.is(true));

        Assert.assertNotNull(dataServiceRetryProperties);
        Assert.assertThat(dataServiceRetryProperties.getInitialInterval(), Matchers.is(200L));

        Assert.assertNotNull(healthProperties);
        Assert.assertThat(healthProperties.getMaxCpuLoadPercent(), Matchers.is(33.3));
        Assert.assertThat(healthProperties.getMaxCpuLoadConsecutiveOccurrences(), Matchers.is(5));

        Assert.assertThat(jobsUsersActiveLimitProperties.isEnabled(), Matchers.is(true));
        Assert.assertThat(jobsUsersActiveLimitProperties.getCount(), Matchers.is(15));
        Assert.assertThat(jobsUsersActiveLimitProperties.getUserLimit("Jane"), Matchers.is(100));
        Assert.assertThat(jobsUsersActiveLimitProperties.getUserLimit("John"), Matchers.is(200));
        Assert.assertThat(jobsUsersActiveLimitProperties.getUserLimit("not-special"), Matchers.is(15));
    }
}
