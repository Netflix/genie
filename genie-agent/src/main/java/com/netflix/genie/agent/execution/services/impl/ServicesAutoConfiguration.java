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

import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.services.DownloadService;
import com.netflix.genie.agent.execution.services.FetchingCacheService;
import com.netflix.genie.agent.execution.services.JobMonitorService;
import com.netflix.genie.agent.execution.services.JobSetupService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.properties.AgentProperties;
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import com.netflix.genie.common.internal.configs.AwsAutoConfiguration;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;

import java.io.IOException;

/**
 * Spring auto configuration for the service tier of an Agent process.
 *
 * @author tgianos
 * @since 4.0.0
 */
//TODO this class lacks a test
@Configuration
@AutoConfigureAfter(AwsAutoConfiguration.class)
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
     * @param agentProperties           The agent properties
     * @return A {@link KillServiceImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(KillService.class)
    public KillService killService(
        final ApplicationEventPublisher applicationEventPublisher,
        final AgentProperties agentProperties
    ) {
        return new KillServiceImpl(applicationEventPublisher, agentProperties);
    }

    /**
     * Provide a lazy {@link JobSetupService} bean if one hasn't already been defined.
     *
     * @param downloadService the download service
     * @return A {@link JobSetupServiceImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(JobSetupService.class)
    public JobSetupService jobSetupService(
        final DownloadService downloadService
    ) {
        return new JobSetupServiceImpl(downloadService);
    }

    /**
     * Provide a lazy {@link JobMonitorService} bean if one hasn't already been defined.
     *
     * @param killService            the kill service
     * @param manifestCreatorService the manifest creator service
     * @param taskScheduler          the task scheduler
     * @param agentProperties        the agent properties
     * @return A {@link JobMonitorServiceImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(JobMonitorService.class)
    public JobMonitorServiceImpl jobMonitorService(
        final KillService killService,
        final JobDirectoryManifestCreatorService manifestCreatorService,
        @Qualifier("sharedAgentTaskScheduler") final TaskScheduler taskScheduler,
        final AgentProperties agentProperties
    ) {
        return new JobMonitorServiceImpl(killService, manifestCreatorService, taskScheduler, agentProperties);
    }
}
