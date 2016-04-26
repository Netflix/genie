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

import com.google.common.collect.Lists;
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import com.netflix.genie.web.resources.writers.DefaultDirectoryWriter;
import com.netflix.genie.web.resources.writers.DirectoryWriter;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Configuration for Spring MVC.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
public class MvcConfig extends WebMvcConfigurerAdapter {

    /**
     * {@inheritDoc}
     * <p>
     * Turn off {@literal .} recognition in paths. Needed due to Job id's in paths potentially having '.' as character.
     *
     * @see <a href="http://stackoverflow.com/a/23938850">Stack Overflow Issue Answer From Dave Syer</a>
     */
    @Override
    public void configurePathMatch(final PathMatchConfigurer configurer) {
        configurer.setUseRegisteredSuffixPatternMatch(true);
    }

    /**
     * Get a resource loader.
     *
     * @return a DefaultResourceLoader
     */
    @Bean
    @ConditionalOnMissingBean
    public ResourceLoader resourceLoader() {
        return new DefaultResourceLoader();
    }

    /**
     * Get the hostname for this application. This is the default fallback implementation if no other bean with
     * id hostname has been created by another profile.
     *
     * @return The hostname calculated from InetAddress
     * @throws UnknownHostException When the host can't be calculated
     * @see InetAddress#getCanonicalHostName()
     */
    @Bean
    @ConditionalOnMissingBean
    public String hostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    /**
     * Get an HttpClient for calling between Genie nodes.
     *
     * @return The http client to use
     */
    @Bean
    @ConditionalOnMissingBean
    public HttpClient genieMvcHttpClient() {
        return HttpClients.createDefault();
    }

    /**
     * Get the directory writer to use.
     *
     * @return A default directory writer
     */
    @Bean
    @ConditionalOnMissingBean
    public DirectoryWriter directoryWriter() {
        return new DefaultDirectoryWriter();
    }

    /**
     * Get the jobs dir as a Spring Resource. Will create if it doesn't exist.
     *
     * @param resourceLoader  The resource loader to use
     * @param jobsDirLocation The location of the job dir
     * @return The job dir as a resource
     * @throws IOException on error reading or creating the directory
     */
    @Bean
    @ConditionalOnMissingBean
    public Resource jobsDir(
        final ResourceLoader resourceLoader,
        @Value("${genie.jobs.dir.location}") final String jobsDirLocation
    ) throws IOException {
        final Resource tmpJobsDirResource = resourceLoader.getResource(jobsDirLocation);
        if (tmpJobsDirResource.exists() && !tmpJobsDirResource.getFile().isDirectory()) {
            throw new IllegalStateException(jobsDirLocation + " exists but isn't a directory. Unable to continue");
        }

        // We want the resource to end in a slash for use later in the generation of URL's
        final String slash = "/";
        String localJobsDir = jobsDirLocation;
        if (!jobsDirLocation.endsWith(slash)) {
            localJobsDir = localJobsDir + slash;
        }
        final Resource jobsDirResource = resourceLoader.getResource(localJobsDir);

        if (!jobsDirResource.exists()) {
            final File file = jobsDirResource.getFile();
            if (!file.mkdirs()) {
                throw new IllegalStateException(
                    "Unable to create jobs directory " + jobsDirLocation + " and it doesn't exist."
                );
            }
        }

        return jobsDirResource;
    }

    /**
     * Get a static resource handler for Genie Jobs.
     *
     * @param directoryWriter The directory writer to use for converting directory resources
     * @param context         The spring application context
     * @param jobsDir         The location the user is requesting the jobs be stored
     * @return The genie resource http request handler.
     */
    @Bean
    @ConditionalOnMissingBean
    public GenieResourceHttpRequestHandler genieResourceHttpRequestHandler(
        final DirectoryWriter directoryWriter,
        final ApplicationContext context,
        final Resource jobsDir
    ) {
        final GenieResourceHttpRequestHandler handler = new GenieResourceHttpRequestHandler(directoryWriter);
        handler.setApplicationContext(context);
        handler.setLocations(Lists.newArrayList(jobsDir));

        return handler;
    }
}
