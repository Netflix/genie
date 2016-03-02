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
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import com.netflix.genie.web.resources.writers.DefaultDirectoryWriter;
import com.netflix.genie.web.resources.writers.DirectoryWriter;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
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
public class MvcConfigUnitTests {

    private MvcConfig mvcConfig;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.mvcConfig = new MvcConfig();
    }

    /**
     * Make sure the suffix pattern matcher is turned off.
     */
    @Test
    public void doesTurnOffSuffixMatcher() {
        final PathMatchConfigurer configurer = Mockito.mock(PathMatchConfigurer.class);
        this.mvcConfig.configurePathMatch(configurer);
        Mockito.verify(configurer, Mockito.times(1)).setUseRegisteredSuffixPatternMatch(true);
    }

    /**
     * Make sure we get a valid resource loader.
     */
    @Test
    public void canGetResourceLoader() {
        Assert.assertTrue(this.mvcConfig.resourceLoader() instanceof DefaultResourceLoader);
    }

    /**
     * Make sure we get the correct host.
     *
     * @throws UnknownHostException When the host can't be calculated
     */
    @Test
    public void canGetHostname() throws UnknownHostException {
        final String expectedHostname = InetAddress.getLocalHost().getCanonicalHostName();
        Assert.assertThat(this.mvcConfig.hostname(), Matchers.is(expectedHostname));
    }

    /**
     * Make sure we get a valid Http client to use.
     */
    @Test
    public void canGetHttpClient() {
        Assert.assertNotNull(this.mvcConfig.genieMvcHttpClient());
    }

    /**
     * Make sure the default implementation of a directory writer is used in this default configuration.
     */
    @Test
    public void canGetDirectoryWriter() {
        Assert.assertTrue(this.mvcConfig.directoryWriter() instanceof DefaultDirectoryWriter);
    }

    /**
     * Test to make sure we can't create a Http Request handler if the directory can't be created when the input jobs
     * dir is invalid in any way.
     *
     * @throws IOException On error
     */
    @Test
    public void cantGetGenieResourceHttpRequestHandlerWhenJobsDirInvalid() throws IOException {
        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        final DirectoryWriter directoryWriter = Mockito.mock(DirectoryWriter.class);
        final ApplicationContext context = Mockito.mock(ApplicationContext.class);
        final String jobsDirLocation = UUID.randomUUID().toString();

        final Resource tmpResource = Mockito.mock(Resource.class);
        Mockito.when(resourceLoader.getResource(jobsDirLocation)).thenReturn(tmpResource);
        Mockito.when(tmpResource.exists()).thenReturn(true);

        final File file = Mockito.mock(File.class);
        Mockito.when(tmpResource.getFile()).thenReturn(file);
        Mockito.when(file.isDirectory()).thenReturn(false);

        try {
            this.mvcConfig.genieResourceHttpRequestHandler(resourceLoader, directoryWriter, context, jobsDirLocation);
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
            this.mvcConfig.genieResourceHttpRequestHandler(resourceLoader, directoryWriter, context, jobsDirLocation);
            Assert.fail();
        } catch (final IllegalStateException ise) {
            Assert.assertThat(
                ise.getMessage(),
                Matchers.is("Unable to create jobs directory " + jobsDirLocation + " and it doesn't exist.")
            );
        }
    }

    /**
     * Make sure we can get a valid GenieResourceHttpRequest when all conditions are met.
     *
     * @throws IOException for any problem
     */
    @Test
    public void canGetGenieResourceHttpRequest() throws IOException {
        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        final DirectoryWriter directoryWriter = Mockito.mock(DirectoryWriter.class);
        final ApplicationContext context = Mockito.mock(ApplicationContext.class);
        final String jobsDirLocation = UUID.randomUUID().toString() + "/";

        final Resource jobsDirResource = Mockito.mock(Resource.class);
        Mockito.when(resourceLoader.getResource(jobsDirLocation)).thenReturn(jobsDirResource);
        Mockito.when(jobsDirResource.exists()).thenReturn(true);

        final File file = Mockito.mock(File.class);
        Mockito.when(jobsDirResource.getFile()).thenReturn(file);
        Mockito.when(file.isDirectory()).thenReturn(true);

        final GenieResourceHttpRequestHandler handler
            = this.mvcConfig.genieResourceHttpRequestHandler(resourceLoader, directoryWriter, context, jobsDirLocation);
        Assert.assertNotNull(handler);
        Assert.assertThat(handler.getApplicationContext(), Matchers.is(context));
        Assert.assertThat(handler.getLocations(), Matchers.hasSize(1));
        Assert.assertThat(handler.getLocations(), Matchers.contains(jobsDirResource));
    }
}
