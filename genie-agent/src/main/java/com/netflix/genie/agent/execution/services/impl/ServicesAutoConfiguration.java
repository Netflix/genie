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
package com.netflix.genie.agent.execution.services.impl;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.services.ArchivalService;
import com.netflix.genie.agent.execution.services.DownloadService;
import com.netflix.genie.agent.execution.services.FetchingCacheService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.services.LaunchJobService;
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.aws.autoconfigure.context.ContextCredentialsAutoConfiguration;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;

import java.io.IOException;

/**
 * Spring auto configuration for the service tier of an Agent process.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@AutoConfigureAfter(ContextCredentialsAutoConfiguration.class)
@Slf4j
public class ServicesAutoConfiguration {

    /**
     * Provide a lazy {@link DownloadService} bean if one hasn't already been defined.
     *
     * @param fetchingCacheService The cache service to use
     * @return A {@link DownloadServiceImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(DownloadService.class)
    public DownloadService downloadService(final FetchingCacheService fetchingCacheService) {
        return new DownloadServiceImpl(fetchingCacheService);
    }

    /**
     * Provide a lazy {@link FetchingCacheService} instance if one hasn't already been defined.
     *
     * @param resourceLoader  The Spring Resource loader to use
     * @param cacheArguments  The cache command line arguments to use
     * @param fileLockFactory The file lock factory to use
     * @param taskExecutor    The task executor to use
     * @return A {@link FetchingCacheServiceImpl} instance
     * @throws IOException On error creating the instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(FetchingCacheService.class)
    public FetchingCacheService fetchingCacheService(
        final ResourceLoader resourceLoader,
        final ArgumentDelegates.CacheArguments cacheArguments,
        final FileLockFactory fileLockFactory,
        @Qualifier("sharedAgentTaskExecutor") final TaskExecutor taskExecutor
    ) throws IOException {
        return new FetchingCacheServiceImpl(
            resourceLoader,
            cacheArguments,
            fileLockFactory,
            taskExecutor
        );
    }

    /**
     * Provide a lazy {@link KillService} bean if one hasn't already been defined.
     *
     * @param applicationEventPublisher The Spring event publisher to use
     * @return A {@link KillServiceImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(KillService.class)
    public KillService killService(final ApplicationEventPublisher applicationEventPublisher) {
        return new KillServiceImpl(applicationEventPublisher);
    }

    /**
     * Provide a lazy {@link LaunchJobService} bean if one hasn't already been defined.
     *
     * @return A {@link LaunchJobServiceImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(LaunchJobService.class)
    public LaunchJobService launchJobService() {
        return new LaunchJobServiceImpl();
    }

    /**
     * Provide a lazy S3 based {@link ArchivalService} bean if AWS credentials are present in the context.
     *
     * @param awsCredentialsProvider The credentials provider to use
     * @return A {@link S3ArchivalServiceImpl} instance if credentials are valid else a {@link NoOpArchivalServiceImpl}
     */
    @Bean
    @Lazy
    @ConditionalOnBean(AWSCredentialsProvider.class)
    public ArchivalService archivalService(final AWSCredentialsProvider awsCredentialsProvider) {
        /*
         * TODO: Spring Cloud AWS always provides a credentials provider once it is on the classpath.
         *
         * For this reason this block exists to proactively verify that the credentials provided will be valid at
         * runtime in order to create a working S3 client later on. If the credentials don't work this will fall back
         * to creating a No Op Archival service implementation
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

        /*
         * TODO: This is a quick and dirty solution to get archival working. Fix/replace.
         *
         * For this to be a property solution we'd need to consider things like:
         * - Role assumption
         * - S3 Client pooling resources (thread pool)
         * - Sharing of S3 client within app context
         * - Exposing options for users
         * - Whether archival is a system dependency and therefore we need one S3 client to upload to the "genie"
         *   managed location and another to place a copy where the user specifies
         * - Probably more stuff I can't think about right now
         */

        // Take all the defaults just override the Credentials Provider in case something special was done
        final AmazonS3 amazonS3 = AmazonS3ClientBuilder
            .standard()
            .withCredentials(awsCredentialsProvider)
            .build();

        return new S3ArchivalServiceImpl(amazonS3);
    }

    /**
     * Provide a lazy {@link ArchivalService} bean if one does not already exist.
     *
     * @return A {@link NoOpArchivalServiceImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(ArchivalService.class)
    public ArchivalService defaultArchivalService() {
        return new NoOpArchivalServiceImpl();
    }
}
