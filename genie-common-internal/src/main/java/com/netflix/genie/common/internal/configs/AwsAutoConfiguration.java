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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.AwsRegionProvider;
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.aws.s3.S3ProtocolResolver;
import com.netflix.genie.common.internal.aws.s3.S3ProtocolResolverRegistrar;
import com.netflix.genie.common.internal.services.JobArchiver;
import com.netflix.genie.common.internal.services.impl.S3JobArchiverImpl;

import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ProtocolResolver;

@Configuration
@EnableConfigurationProperties
@ConditionalOnBean(AWSCredentialsProvider.class)
@Slf4j
public class AwsAutoConfiguration {
    /**
     * Constant for the precedence of the S3 job archive implementation for others to reference if need be.
     *
     * @see Ordered
     */
    public static final int S3_JOB_ARCHIVER_PRECEDENCE = Ordered.HIGHEST_PRECEDENCE + 10;

    /**
     * Provide a lazy {@link S3ClientFactory} instance if one is needed by the system.
     *
     * @param awsCredentialsProvider The {@link AWSCredentialsProvider} to use
     * @param awsRegionProvider      The {@link AwsRegionProvider} to use
     * @param environment            The Spring application {@link Environment} to bind properties from
     * @return A {@link S3ClientFactory} instance
     */
    @Bean
    @ConditionalOnMissingBean(S3ClientFactory.class)
    public S3ClientFactory s3ClientFactory(
        final AWSCredentialsProvider awsCredentialsProvider,
        final AwsRegionProvider awsRegionProvider,
        final Environment environment
    ) {
        return new S3ClientFactory(awsCredentialsProvider, awsRegionProvider, environment);
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
     * Provide an implementation of {@link JobArchiver} to handle archiving
     * to S3.
     *
     * @param s3ClientFactory The factory for creating S3 clients
     * @return A {@link S3JobArchiverImpl} instance
     */
    @Bean
    @Order(S3_JOB_ARCHIVER_PRECEDENCE)
    public S3JobArchiverImpl s3JobArchiver(final S3ClientFactory s3ClientFactory) {
        return new S3JobArchiverImpl(s3ClientFactory);
    }
}
