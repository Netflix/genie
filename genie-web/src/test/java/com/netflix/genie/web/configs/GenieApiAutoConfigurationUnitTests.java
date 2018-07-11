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

import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.properties.HttpProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import com.netflix.genie.web.resources.writers.DefaultDirectoryWriter;
import com.netflix.genie.web.resources.writers.DirectoryWriter;
import com.netflix.genie.web.services.JobFileService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Unit tests for the MvcConfig beans.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieApiAutoConfigurationUnitTests {

    private GenieApiAutoConfiguration genieApiAutoConfiguration;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.genieApiAutoConfiguration = new GenieApiAutoConfiguration();
    }

    /**
     * Make sure the suffix pattern matcher is turned off.
     */
    @Test
    public void doesTurnOffSuffixMatcher() {
        final PathMatchConfigurer configurer = Mockito.mock(PathMatchConfigurer.class);
        this.genieApiAutoConfiguration.configurePathMatch(configurer);
        Mockito.verify(configurer, Mockito.times(1)).setUseRegisteredSuffixPatternMatch(true);
    }

    /**
     * Make sure we get a valid resource loader.
     */
    @Test
    public void canGetResourceLoader() {
        Assert.assertTrue(this.genieApiAutoConfiguration.resourceLoader() instanceof DefaultResourceLoader);
    }

    /**
     * Make sure we get the correct host.
     *
     * @throws UnknownHostException When the host can't be calculated
     */
    @Test
    public void canGetGenieHostInfo() throws UnknownHostException {
        final String expectedHostname = InetAddress.getLocalHost().getCanonicalHostName();
        Assert.assertThat(this.genieApiAutoConfiguration.genieHostInfo().getHostname(), Matchers.is(expectedHostname));
    }

    /**
     * Make sure we get a valid rest template to use.
     */
    @Test
    public void canGetRestTemplate() {
        Assert.assertNotNull(
            this.genieApiAutoConfiguration.genieRestTemplate(new HttpProperties(), new RestTemplateBuilder())
        );
    }

    /**
     * Make sure the default implementation of a directory writer is used in this default configuration.
     */
    @Test
    public void canGetDirectoryWriter() {
        Assert.assertTrue(this.genieApiAutoConfiguration.directoryWriter() instanceof DefaultDirectoryWriter);
    }

    /**
     * Test to make sure we can't create a jobs dir resource if the directory can't be created when the input jobs
     * dir is invalid in any way.
     *
     * @throws IOException On error
     */
    @Test
    public void cantGetJobsDirWhenJobsDirInvalid() throws IOException {
        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        final String jobsDirLocation = UUID.randomUUID().toString();
        final JobsProperties jobsProperties = new JobsProperties();
        jobsProperties.getLocations().setJobs(jobsDirLocation);

        final Resource tmpResource = Mockito.mock(Resource.class);
        Mockito.when(resourceLoader.getResource(jobsDirLocation)).thenReturn(tmpResource);
        Mockito.when(tmpResource.exists()).thenReturn(true);

        final File file = Mockito.mock(File.class);
        Mockito.when(tmpResource.getFile()).thenReturn(file);
        Mockito.when(file.isDirectory()).thenReturn(false);

        try {
            this.genieApiAutoConfiguration.jobsDir(resourceLoader, jobsProperties);
            Assert.fail();
        } catch (final IllegalStateException ise) {
            Assert.assertThat(
                ise.getMessage(),
                Matchers.is(jobsDirLocation + " exists but isn't a directory. Unable to continue")
            );
        }

        final String localJobsDir = jobsDirLocation + "/";
        Mockito.when(file.isDirectory()).thenReturn(true);
        final Resource jobsDirResource = Mockito.mock(Resource.class);
        Mockito.when(resourceLoader.getResource(localJobsDir)).thenReturn(jobsDirResource);
        Mockito.when(tmpResource.exists()).thenReturn(false);

        Mockito.when(jobsDirResource.exists()).thenReturn(false);
        Mockito.when(jobsDirResource.getFile()).thenReturn(file);
        Mockito.when(file.mkdirs()).thenReturn(false);

        try {
            this.genieApiAutoConfiguration.jobsDir(resourceLoader, jobsProperties);
            Assert.fail();
        } catch (final IllegalStateException ise) {
            Assert.assertThat(
                ise.getMessage(),
                Matchers.is("Unable to create jobs directory " + jobsDirLocation + " and it doesn't exist.")
            );
        }
    }

    /**
     * Make sure we can get a valid job resource when all conditions are met.
     *
     * @throws IOException for any problem
     */
    @Test
    public void canGetJobsDir() throws IOException {
        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        final String jobsDirLocation = UUID.randomUUID().toString() + "/";
        final JobsProperties jobsProperties = new JobsProperties();
        jobsProperties.getLocations().setJobs(jobsDirLocation);

        final Resource jobsDirResource = Mockito.mock(Resource.class);
        Mockito.when(resourceLoader.getResource(jobsDirLocation)).thenReturn(jobsDirResource);
        Mockito.when(jobsDirResource.exists()).thenReturn(true);

        final File file = Mockito.mock(File.class);
        Mockito.when(jobsDirResource.getFile()).thenReturn(file);
        Mockito.when(file.isDirectory()).thenReturn(true);

        final Resource jobsDir = this.genieApiAutoConfiguration.jobsDir(resourceLoader, jobsProperties);
        Assert.assertNotNull(jobsDir);
    }

    /**
     * Make sure we can get a valid genieResourceHttpRequestHandler.
     */
    @Test
    public void canGetGenieResourceHttpRequestHandler() {
        final DirectoryWriter directoryWriter = Mockito.mock(DirectoryWriter.class);
        final ApplicationContext context = Mockito.mock(ApplicationContext.class);
        final JobFileService jobFileService = Mockito.mock(JobFileService.class);

        final GenieResourceHttpRequestHandler handler
            = this.genieApiAutoConfiguration.genieResourceHttpRequestHandler(directoryWriter, context, jobFileService);
        Assert.assertThat(handler.getApplicationContext(), Matchers.is(context));
    }
}
