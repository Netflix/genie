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
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import com.netflix.genie.web.resources.writers.DefaultDirectoryWriter;
import com.netflix.genie.web.resources.writers.DirectoryWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

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
     * Get RestTemplate for calling between Genie nodes.
     *
     * @param httpConnectTimeout http connection timeout in milliseconds
     * @param httpReadTimeout    http read timeout in milliseconds
     * @return The rest template to use
     */
    @Bean(name = "genieRestTemplate")
    public RestTemplate restTemplate(
        @Value("${genie.http.connect.timeout:2000}") final int httpConnectTimeout,
        @Value("${genie.http.connect.timeout:10000}") final int httpReadTimeout
    ) {
        final HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(httpConnectTimeout);
        factory.setReadTimeout(httpReadTimeout);
        return new RestTemplate(factory);
    }

    /**
     * Get RetryTemplate.
     *
     * @param noOfRetries     number of retries
     * @param initialInterval initial interval for the backoff policy
     * @param maxInterval     maximum interval for the backoff policy
     * @return The retry template to use
     */
    @Bean(name = "genieRetryTemplate")
    public RetryTemplate retryTemplate(
        @Value("${genie.retry.noOfRetries:5}") final int noOfRetries,
        @Value("${genie.retry.initialInterval:10000}") final int initialInterval,
        @Value("${genie.retry.maxInterval:60000}") final int maxInterval
    ) {
        final RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new SimpleRetryPolicy(noOfRetries,
            Collections.singletonMap(Exception.class, true)));
        final ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialInterval);
        backOffPolicy.setMaxInterval(maxInterval);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
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
     * @param resourceLoader The resource loader to use
     * @param jobsProperties The jobs properties to use
     * @return The job dir as a resource
     * @throws IOException on error reading or creating the directory
     */
    @Bean
    @ConditionalOnMissingBean
    public Resource jobsDir(
        final ResourceLoader resourceLoader,
        final JobsProperties jobsProperties
    ) throws IOException {
        final String jobsDirLocation = jobsProperties.getLocations().getJobs();
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
