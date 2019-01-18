/*
 *
 *  Copyright 2019 Netflix, Inc.
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
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.configs.AwsAutoConfiguration;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.impl.NoOpJobArchiveServiceImpl;
import com.netflix.genie.common.internal.services.impl.S3JobArchiveServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * AWS related auto configuration specific to the Agent process.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties
@AutoConfigureAfter(
    {
        AwsAutoConfiguration.class
    }
)
@ConditionalOnBean(AWSCredentialsProvider.class)
@Slf4j
public class AgentAwsAutoConfiguration {

    /**
     * Provide a lazy S3 based {@link JobArchiveService} bean if AWS credentials are present in the context.
     *
     * @param awsCredentialsProvider The credentials provider to use
     * @param s3ClientFactory        The {@link S3ClientFactory} to use to get clients for buckets
     * @return A {@link S3JobArchiveServiceImpl} instance if credentials are valid else a
     * {@link NoOpJobArchiveServiceImpl}
     */
    @Bean
    @Lazy
    public JobArchiveService archivalService(
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

            return new NoOpJobArchiveServiceImpl();
        }

        return new S3JobArchiveServiceImpl(s3ClientFactory);
    }
}
