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

import com.amazonaws.services.s3.AmazonS3;
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.services.ArchivalService;
import com.netflix.genie.agent.execution.services.DownloadService;
import com.netflix.genie.agent.execution.services.FetchingCacheService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.services.LaunchJobService;
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
     * Provide a lazy {@link ArchivalService} bean if AmazonS3 client exists.
     * @param amazonS3 Amazon S3 client instance
     * @return A {@link ArchivalService} instance
     */
    @Bean
    @Lazy
    @ConditionalOnBean(AmazonS3.class)
    public ArchivalService archivalService(final AmazonS3 amazonS3) {
        return new S3ArchivalServiceImpl(amazonS3);
    }

    /**
     * Provide a lazy {@link ArchivalService} bean if one does not already exist.
     * @return A {@link ArchivalService} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(ArchivalService.class)
    public ArchivalService archivalService() {
        return new NoOpArchivalServiceImpl();
    }
}
