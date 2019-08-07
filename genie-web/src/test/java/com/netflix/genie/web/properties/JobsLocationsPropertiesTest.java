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

import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;

/**
 * Unit tests for JobsLocationsProperties.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JobsLocationsPropertiesTest {

    private static final String SYSTEM_TMP_DIR = System.getProperty("java.io.tmpdir", "/tmp/");
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
        Assertions
            .assertThat(this.properties.getArchives())
            .isEqualTo(URI.create("file://" + SYSTEM_TMP_DIR + "genie/archives/"));
        Assertions
            .assertThat(this.properties.getAttachments())
            .isEqualTo(URI.create("file://" + SYSTEM_TMP_DIR + "genie/attachments/"));
        Assertions
            .assertThat(this.properties.getJobs())
            .isEqualTo(URI.create("file://" + SYSTEM_TMP_DIR + "genie/jobs/"));
    }

    /**
     * Test setting the archives location.
     */
    @Test
    public void canSetArchivesLocation() {
        final URI location = URI.create("file:/" + UUID.randomUUID().toString());
        this.properties.setArchives(location);
        Assert.assertThat(this.properties.getArchives(), Matchers.is(location));
    }

    /**
     * Test setting the attachments location.
     */
    @Test
    public void canSetAttachmentsLocation() {
        final URI location = URI.create("file:/" + UUID.randomUUID().toString());
        this.properties.setAttachments(location);
        Assert.assertThat(this.properties.getAttachments(), Matchers.is(location));
    }

    /**
     * Test setting the jobs dir location.
     */
    @Test
    public void canSetJobsLocation() {
        final URI location = URI.create("file:/" + UUID.randomUUID().toString());
        this.properties.setJobs(location);
        Assert.assertThat(this.properties.getJobs(), Matchers.is(location));
    }
}
