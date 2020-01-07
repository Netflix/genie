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

import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.web.properties.HttpProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.properties.RetryProperties;
import com.netflix.genie.web.resources.writers.DefaultDirectoryWriter;
import com.netflix.genie.web.resources.writers.DirectoryWriter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.filter.CharacterEncodingFilter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

/**
 * Configuration for external API tier.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        HttpProperties.class,
        RetryProperties.class
    }
)
public class ApisAutoConfiguration {

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
     * Get RestTemplate for calling between Genie nodes.
     *
     * @param httpProperties      The properties related to Genie's HTTP client configuration
     * @param restTemplateBuilder The Spring REST template builder to use
     * @return The rest template to use
     */
    @Bean
    @ConditionalOnMissingBean(name = "genieRestTemplate")
    public RestTemplate genieRestTemplate(
        final HttpProperties httpProperties,
        final RestTemplateBuilder restTemplateBuilder
    ) {
        return restTemplateBuilder
            .setConnectTimeout(Duration.of(httpProperties.getConnect().getTimeout(), ChronoUnit.MILLIS))
            .setReadTimeout(Duration.of(httpProperties.getRead().getTimeout(), ChronoUnit.MILLIS))
            .build();
    }

    /**
     * Get RetryTemplate.
     *
     * @param retryProperties The http retry properties to use
     * @return The retry template to use
     */
    @Bean
    @ConditionalOnMissingBean(name = "genieRetryTemplate")
    public RetryTemplate genieRetryTemplate(final RetryProperties retryProperties) {
        final RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(
            new SimpleRetryPolicy(
                retryProperties.getNoOfRetries(),
                Collections.singletonMap(Exception.class, true)
            )
        );
        final ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(retryProperties.getInitialInterval());
        backOffPolicy.setMaxInterval(retryProperties.getMaxInterval());
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
    public DefaultDirectoryWriter directoryWriter() {
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
    @ConditionalOnMissingBean(name = "jobsDir", value = Resource.class)
    public Resource jobsDir(
        final ResourceLoader resourceLoader,
        final JobsProperties jobsProperties
    ) throws IOException {
        final String jobsDirLocation = jobsProperties.getLocations().getJobs().toString();
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

    /**
     * Customizer for {@link com.fasterxml.jackson.databind.ObjectMapper} used by controllers.
     *
     * @return an {@link Jackson2ObjectMapperBuilderCustomizer}
     */
    @Bean
    public Jackson2ObjectMapperBuilderCustomizer apisObjectMapperCustomizer() {
        return builder -> {
            // Register the same PropertyFilters that are registered in the static GenieObjectMapper
            builder.filters(GenieObjectMapper.FILTER_PROVIDER);
        };
    }
}
