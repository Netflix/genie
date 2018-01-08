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

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

/**
 * Unit tests for JobsLocationsProperties.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobsLocationsPropertiesUnitTests {

    private JobsLocationsProperties properties;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.properties = new JobsLocationsProperties();
    }

    /**
     * Make sure defaults are set.
     */
    @Test
    public void canConstruct() {
        Assert.assertThat(this.properties.getArchives(), Matchers.is("file:///tmp/genie/archives/"));
        Assert.assertThat(this.properties.getAttachments(), Matchers.is("file:///tmp/genie/attachments/"));
        Assert.assertThat(this.properties.getJobs(), Matchers.is("file:///tmp/genie/jobs/"));
    }

    /**
     * Test setting the archives location.
     */
    @Test
    public void canSetArchivesLocation() {
        final String location = UUID.randomUUID().toString();
        this.properties.setArchives(location);
        Assert.assertThat(this.properties.getArchives(), Matchers.is(location));
    }

    /**
     * Test setting the attachments location.
     */
    @Test
    public void canSetAttachmentsLocation() {
        final String location = UUID.randomUUID().toString();
        this.properties.setAttachments(location);
        Assert.assertThat(this.properties.getAttachments(), Matchers.is(location));
    }

    /**
     * Test setting the jobs dir location.
     */
    @Test
    public void canSetJobsLocation() {
        final String location = UUID.randomUUID().toString();
        this.properties.setJobs(location);
        Assert.assertThat(this.properties.getJobs(), Matchers.is(location));
    }
}
