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
package com.netflix.genie.common.internal.configs;

import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.aws.s3.S3ProtocolResolver;
import com.netflix.genie.common.internal.aws.s3.S3ProtocolResolverRegistrar;
import com.netflix.genie.common.internal.aws.s3.S3ResourceLoaderProperties;
import com.netflix.genie.common.internal.aws.s3.S3TransferManagerFactory;
import com.netflix.genie.common.internal.services.JobArchiver;
import com.netflix.genie.common.internal.services.impl.S3JobArchiverImpl;
import io.awspring.cloud.autoconfigure.core.CredentialsProviderAutoConfiguration;
import io.awspring.cloud.autoconfigure.core.RegionProviderAutoConfiguration;
import io.awspring.cloud.autoconfigure.core.RegionProperties;
import io.awspring.cloud.autoconfigure.s3.properties.S3Properties;
import io.awspring.cloud.autoconfigure.s3.S3AutoConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

/**
 * Spring Boot auto configuration for AWS related beans for the Genie Agent. Should be configured after all the
 * Spring Cloud AWS context configurations are complete.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties({S3ResourceLoaderProperties.class, S3Properties.class})
@AutoConfigureAfter(
    {
        CredentialsProviderAutoConfiguration.class,
        RegionProviderAutoConfiguration.class,
        S3AutoConfiguration.class
    }
)
@ConditionalOnBean(AwsCredentialsProvider.class)
@Slf4j
public class AwsAutoConfiguration {

    /**
     * Constant for the precedence of the S3 job archive implementation for others to reference if need be.
     *
     * @see Ordered
     */
    public static final int S3_JOB_ARCHIVER_PRECEDENCE = Ordered.HIGHEST_PRECEDENCE + 10;

    /**
     * Get an AWS region provider instance.
     *
     * @param regionProperties The cloud.aws.region.* properties
     * @return A region provider based on whether static was set by user, else auto, else default of us-east-1
     */
    @Bean
    @Primary
    public AwsRegionProvider awsRegionProvider(final RegionProperties regionProperties) {
        final String staticRegion = regionProperties.getStatic();
        if (StringUtils.isNotBlank(staticRegion)) {
            // Make sure we have a valid region. Will throw runtime exception if not.
            return () -> Region.of(staticRegion);
        } else {
            // Try DefaultAwsRegionProviderChain, but fall back to us-east-1 if it fails
            try {
                DefaultAwsRegionProviderChain providerChain = new DefaultAwsRegionProviderChain();
                Region region = providerChain.getRegion();
                return () -> region;
            } catch (Exception e) {
                log.warn("Failed to get region from DefaultAwsRegionProviderChain, falling back to us-east-1", e);
                return () -> Region.US_EAST_1;
            }
        }
    }

    /**
     * Provide a lazy {@link S3ClientFactory} instance if one is needed by the system.
     *
     * @param awsCredentialsProvider The {@link AwsCredentialsProvider} to use
     * @param awsRegionProvider      The {@link AwsRegionProvider} to use
     * @param environment            The Spring application {@link Environment} to bind properties from
     * @return A {@link S3ClientFactory} instance
     */
    @Bean
    @ConditionalOnMissingBean(S3ClientFactory.class)
    public S3ClientFactory s3ClientFactory(
        final AwsCredentialsProvider awsCredentialsProvider,
        final AwsRegionProvider awsRegionProvider,
        final Environment environment
    ) {
        return new S3ClientFactory(awsCredentialsProvider, awsRegionProvider, environment);
    }

    /**
     * Provide a configuration properties bean for Spring Cloud resource loader properties if for whatever reason
     * the {@link S3AutoConfiguration} isn't applied by the agent app.
     *
     * @return A {@link S3Properties} instance with the bindings from cloud.aws.loader values
     */
    @Bean
    @ConditionalOnMissingBean(S3Properties.class)
    @ConfigurationProperties(S3Properties.PREFIX)
    public S3Properties s3Properties() {
        return new S3Properties();
    }

    /**
     * Provide a protocol resolver which will allow resources with s3:// prefixes
     *
     * @param resourceLoaderProperties The {@link S3ResourceLoaderProperties} instance to use
     * @param s3ClientFactory          The {@link S3ClientFactory} instance to use
     * @return A {@link S3ProtocolResolver} instance
     */
    @Bean
    @ConditionalOnMissingBean(S3ProtocolResolver.class)
    public S3ProtocolResolver s3ProtocolResolver(
        final S3ResourceLoaderProperties resourceLoaderProperties,
        final S3ClientFactory s3ClientFactory
    ) {
        final ThreadPoolTaskExecutor s3TaskExecutor = new ThreadPoolTaskExecutor();
        s3TaskExecutor.setCorePoolSize(resourceLoaderProperties.getCorePoolSize());
        s3TaskExecutor.setMaxPoolSize(resourceLoaderProperties.getMaxPoolSize());
        s3TaskExecutor.setQueueCapacity(resourceLoaderProperties.getQueueCapacity());
        s3TaskExecutor.setThreadGroupName("Genie-S3-Resource-Loader-Thread-Pool");
        s3TaskExecutor.setThreadNamePrefix("S3-resource-loader-thread");
        return new S3ProtocolResolver(s3ClientFactory, s3TaskExecutor);
    }

    /**
     * Configurer bean which will add the {@link S3ProtocolResolver} to the set of {@link ProtocolResolver} in the
     * application context.
     *
     * @param s3ProtocolResolver The implementation of {@link S3ProtocolResolver} to use
     * @return A {@link S3ProtocolResolverRegistrar} instance
     */
    @Bean
    @ConditionalOnMissingBean(S3ProtocolResolverRegistrar.class)
    public S3ProtocolResolverRegistrar s3ProtocolResolverRegistrar(final S3ProtocolResolver s3ProtocolResolver) {
        return new S3ProtocolResolverRegistrar(s3ProtocolResolver);
    }

    /**
     * Provide a {@link S3TransferManagerFactory} instance if one is needed by the system.
     * This factory is responsible for creating and managing {@link software.amazon.awssdk.transfer.s3.S3TransferManager}
     * instances, which are used for efficient transfer of files to and from S3.
     *
     * @param s3ClientFactory The {@link S3ClientFactory} instance to use for configuration and utilities
     * @return A {@link S3TransferManagerFactory} instance
     */
    @Bean
    @ConditionalOnMissingBean(S3TransferManagerFactory.class)
    public S3TransferManagerFactory s3TransferManagerFactory(final S3ClientFactory s3ClientFactory) {
        return new S3TransferManagerFactory(s3ClientFactory);
    }

    /**
     * Provide an implementation of {@link JobArchiver} to handle archiving
     * to S3.
     *
     * @param s3TransferManagerFactory The factory for creating S3 transfer manager
     * @return A {@link S3JobArchiverImpl} instance
     */
    @Bean
    @Order(S3_JOB_ARCHIVER_PRECEDENCE)
    public S3JobArchiverImpl s3JobArchiver(final S3TransferManagerFactory s3TransferManagerFactory) {
        return new S3JobArchiverImpl(s3TransferManagerFactory);
    }
}
