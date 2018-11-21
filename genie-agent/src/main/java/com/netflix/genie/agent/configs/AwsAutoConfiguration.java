/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.agent.configs;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.regions.Regions;
import com.netflix.genie.agent.aws.s3.S3ClientFactory;
import com.netflix.genie.agent.execution.services.ArchivalService;
import com.netflix.genie.agent.execution.services.impl.NoOpArchivalServiceImpl;
import com.netflix.genie.agent.execution.services.impl.S3ArchivalServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.aws.autoconfigure.context.ContextCredentialsAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextInstanceDataAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextRegionProviderAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextResourceLoaderAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextStackAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

import javax.annotation.Nullable;

/**
 * Spring Boot auto configuration for AWS related beans for the Genie Agent. Should be configured after all the
 * Spring Cloud AWS context configurations are complete.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@AutoConfigureAfter(
    {
        ContextCredentialsAutoConfiguration.class,
        ContextInstanceDataAutoConfiguration.class,
        ContextRegionProviderAutoConfiguration.class,
        ContextResourceLoaderAutoConfiguration.class,
        ContextStackAutoConfiguration.class
    }
)
@ConditionalOnBean(AWSCredentialsProvider.class)
@Slf4j
public class AwsAutoConfiguration {

    /**
     * Get an AWS region provider instance. The rules for this basically follow what Spring Cloud AWS does but uses
     * the interface from the AWS SDK instead and provides a sensible default.
     * <p>
     * See: <a href="https://tinyurl.com/y9edl6yr">Spring Cloud AWS Region Documentation</a>
     *
     * @param auto         Binding for the {@code cloud.aws.region.auto} property with default value of {@code true}
     * @param staticRegion Binding for the {@code cloud.aws.region.static} property with default value of {@code null}
     * @return A region provider based on whether static was set by user, else auto, else default of us-east-1
     */
    @Bean
    @ConditionalOnMissingBean(AwsRegionProvider.class)
    public AwsRegionProvider awsRegionProvider(
        @Value("${cloud.aws.region.auto:true}") final boolean auto,
        @Nullable @Value("${cloud.aws.region.static:#{null}}") final String staticRegion
    ) {
        if (StringUtils.isNotBlank(staticRegion)) {
            // Make sure we have a valid region. Will throw runtime exception if not.
            final Regions region = Regions.fromName(staticRegion);
            return new AwsRegionProvider() {
                /**
                 * Always return the static configured region.
                 *
                 * {@inheritDoc}
                 */
                @Override
                public String getRegion() throws SdkClientException {
                    return region.getName();
                }
            };
        } else if (auto) {
            return new DefaultAwsRegionProviderChain();
        } else {
            // Sensible default
            return new AwsRegionProvider() {
                /**
                 * Always default to us-east-1.
                 *
                 * {@inheritDoc}
                 */
                @Override
                public String getRegion() throws SdkClientException {
                    return Regions.US_EAST_1.getName();
                }
            };
        }
    }

    /**
     * Provide a lazy {@link S3ClientFactory} instance if one is needed by the system.
     *
     * @param awsCredentialsProvider The {@link AWSCredentialsProvider} to use
     * @param awsRegionProvider      The {@link AwsRegionProvider} to use
     * @param environment            The Spring application {@link Environment} to bind properties from
     * @return A {@link S3ClientFactory} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(S3ClientFactory.class)
    public S3ClientFactory s3ClientFactory(
        final AWSCredentialsProvider awsCredentialsProvider,
        final AwsRegionProvider awsRegionProvider,
        final Environment environment
    ) {
        return new S3ClientFactory(awsCredentialsProvider, awsRegionProvider, environment);
    }

    /**
     * Provide a lazy S3 based {@link ArchivalService} bean if AWS credentials are present in the context.
     *
     * @param awsCredentialsProvider The credentials provider to use
     * @param s3ClientFactory        The {@link S3ClientFactory} to use to get clients for buckets
     * @return A {@link S3ArchivalServiceImpl} instance if credentials are valid else a {@link NoOpArchivalServiceImpl}
     */
    @Bean
    @Lazy
    public ArchivalService archivalService(
        final AWSCredentialsProvider awsCredentialsProvider,
        final S3ClientFactory s3ClientFactory
    ) {
        /*
         * TODO: Spring Cloud AWS always provides a credentials provider once it is on the classpath.
         *
         * For this reason this block exists to proactively verify that the credentials provided will be valid at
         * runtime in order to create a working S3 client later on. If the credentials don't work this will fall back
         * to creating a No Op Archival service implementation.
         *
         * Long term we should just have one ArchivalServiceImpl which uses the ResourceLoader and this won't be
         * necessary.
         */
        try {
            awsCredentialsProvider.getCredentials();
        } catch (final SdkClientException sdkClientException) {
            log.warn(
                "Attempted to validate AWS credentials and failed due to {}. Falling back to no op implementation",
                sdkClientException.getMessage(),
                sdkClientException
            );

            return new NoOpArchivalServiceImpl();
        }

        return new S3ArchivalServiceImpl(s3ClientFactory);
    }
}
