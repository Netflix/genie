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

import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import com.netflix.genie.web.resources.writers.DefaultDirectoryWriter;
import com.netflix.genie.web.resources.writers.DirectoryWriter;
import com.netflix.genie.web.services.JobFileService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * Configuration for Spring MVC.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
public class MvcConfig implements WebMvcConfigurer {

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
    @ConditionalOnMissingBean(ResourceLoader.class)
    public ResourceLoader resourceLoader() {
        return new DefaultResourceLoader();
    }

    /**
     * Get the {@link GenieHostInfo} for this application. This is the default fallback implementation if no other bean
     * instance of this type has been created.
     *
     * @return The hostname calculated from {@link InetAddress}
     * @throws UnknownHostException  When the host can't be calculated
     * @throws IllegalStateException When an instance can't be created
     * @see InetAddress#getCanonicalHostName()
     */
    @Bean
    @ConditionalOnMissingBean(GenieHostInfo.class)
    public GenieHostInfo genieHostInfo() throws UnknownHostException {
        final String hostname = InetAddress.getLocalHost().getCanonicalHostName();
        if (StringUtils.isNotBlank(hostname)) {
            return new GenieHostInfo(hostname);
        } else {
            throw new IllegalStateException("Unable to create a Genie Host Info instance");
        }
    }

    /**
     * Get RestTemplate for calling between Genie nodes.
     *
     * @param httpConnectTimeout http connection timeout in milliseconds
     * @param httpReadTimeout    http read timeout in milliseconds
     * @return The rest template to use
     */
    @Primary
    @Bean(name = "genieRestTemplate")
    public RestTemplate restTemplate(
        @Value("${genie.http.connect.timeout:2000}") final int httpConnectTimeout,
        @Value("${genie.http.read.timeout:10000}") final int httpReadTimeout
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
     * @param initialInterval initial interval for the back-off policy
     * @param maxInterval     maximum interval for the back-off policy
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
    @ConditionalOnMissingBean(DirectoryWriter.class)
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
     * Get a static resource handler for Genie job logs.
     *
     * @param directoryWriter The directory writer to use for converting directory resources
     * @param context         The spring application context
     * @param jobFileService  The job file service to use
     * @return The genie resource http request handler.
     */
    @Bean
    @ConditionalOnMissingBean(GenieResourceHttpRequestHandler.class)
    public GenieResourceHttpRequestHandler genieResourceHttpRequestHandler(
        final DirectoryWriter directoryWriter,
        final ApplicationContext context,
        final JobFileService jobFileService
    ) {
        final GenieResourceHttpRequestHandler handler
            = new GenieResourceHttpRequestHandler(directoryWriter, jobFileService);
        handler.setApplicationContext(context);

        return handler;
    }

    /**
     * Character encoding filter that forces content-type in response to be UTF-8.
     *
     * @return The encoding filter
     */
    @Bean
    public CharacterEncodingFilter characterEncodingFilter() {
        final CharacterEncodingFilter characterEncodingFilter = new CharacterEncodingFilter();
        characterEncodingFilter.setEncoding(StandardCharsets.UTF_8.name());
        // This effectively obliterates any upstream default and/or encoding detectors
        // As a result, everything is served as UTF-8
        characterEncodingFilter.setForceEncoding(true);
        return characterEncodingFilter;
    }
}
