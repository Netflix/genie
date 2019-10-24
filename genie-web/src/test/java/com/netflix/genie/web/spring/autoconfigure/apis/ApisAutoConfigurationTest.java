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
package com.netflix.genie.web.spring.autoconfigure.apis;

import com.netflix.genie.web.properties.HttpProperties;
import com.netflix.genie.web.properties.JobsProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;

/**
 * Unit tests for {@link ApisAutoConfiguration} beans.
 *
 * @author tgianos
 * @since 3.0.0
 */
class ApisAutoConfigurationTest {

    private ApisAutoConfiguration apisAutoConfiguration;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.apisAutoConfiguration = new ApisAutoConfiguration();
    }

    /**
     * Make sure we get a valid resource loader.
     */
    @Test
    void canGetResourceLoader() {
        Assertions.assertThat(this.apisAutoConfiguration.resourceLoader()).isInstanceOf(DefaultResourceLoader.class);
    }

    /**
     * Make sure we get a valid rest template to use.
     */
    @Test
    void canGetRestTemplate() {
        Assertions
            .assertThat(this.apisAutoConfiguration.genieRestTemplate(new HttpProperties(), new RestTemplateBuilder()))
            .isNotNull();
    }

    /**
     * Make sure the default implementation of a directory writer is used in this default configuration.
     */
    @Test
    void canGetDirectoryWriter() {
        Assertions.assertThat(this.apisAutoConfiguration.directoryWriter()).isNotNull();
    }

    /**
     * Test to make sure we can't create a jobs dir resource if the directory can't be created when the input jobs
     * dir is invalid in any way.
     *
     * @throws IOException On error
     */
    @Test
    void cantGetJobsDirWhenJobsDirInvalid() throws IOException {
        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        final URI jobsDirLocation = URI.create("file:/" + UUID.randomUUID().toString());
        final JobsProperties jobsProperties = JobsProperties.getJobsPropertiesDefaults();
        jobsProperties.getLocations().setJobs(jobsDirLocation);

        final Resource tmpResource = Mockito.mock(Resource.class);
        Mockito.when(resourceLoader.getResource(jobsDirLocation.toString())).thenReturn(tmpResource);
        Mockito.when(tmpResource.exists()).thenReturn(true);

        final File file = Mockito.mock(File.class);
        Mockito.when(tmpResource.getFile()).thenReturn(file);
        Mockito.when(file.isDirectory()).thenReturn(false);

        Assertions
            .assertThatIllegalStateException()
            .isThrownBy(() -> this.apisAutoConfiguration.jobsDir(resourceLoader, jobsProperties))
            .withMessage(jobsDirLocation + " exists but isn't a directory. Unable to continue");

        final String localJobsDir = jobsDirLocation + "/";
        Mockito.when(file.isDirectory()).thenReturn(true);
        final Resource jobsDirResource = Mockito.mock(Resource.class);
        Mockito.when(resourceLoader.getResource(localJobsDir)).thenReturn(jobsDirResource);
        Mockito.when(tmpResource.exists()).thenReturn(false);

        Mockito.when(jobsDirResource.exists()).thenReturn(false);
        Mockito.when(jobsDirResource.getFile()).thenReturn(file);
        Mockito.when(file.mkdirs()).thenReturn(false);

        Assertions
            .assertThatIllegalStateException()
            .isThrownBy(() -> this.apisAutoConfiguration.jobsDir(resourceLoader, jobsProperties))
            .withMessage("Unable to create jobs directory " + jobsDirLocation + " and it doesn't exist.");
    }

    /**
     * Make sure we can get a valid job resource when all conditions are met.
     *
     * @throws IOException for any problem
     */
    @Test
    void canGetJobsDir() throws IOException {
        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        final URI jobsDirLocation = URI.create("file:///" + UUID.randomUUID().toString() + "/");
        final JobsProperties jobsProperties = JobsProperties.getJobsPropertiesDefaults();
        jobsProperties.getLocations().setJobs(jobsDirLocation);

        final Resource jobsDirResource = Mockito.mock(Resource.class);
        Mockito.when(resourceLoader.getResource(jobsDirLocation.toString())).thenReturn(jobsDirResource);
        Mockito.when(jobsDirResource.exists()).thenReturn(true);

        final File file = Mockito.mock(File.class);
        Mockito.when(jobsDirResource.getFile()).thenReturn(file);
        Mockito.when(file.isDirectory()).thenReturn(true);

        final Resource jobsDir = this.apisAutoConfiguration.jobsDir(resourceLoader, jobsProperties);
        Assertions.assertThat(jobsDir).isNotNull();
    }
}
