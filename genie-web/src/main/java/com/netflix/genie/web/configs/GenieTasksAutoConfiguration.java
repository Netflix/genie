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
import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.properties.DiskCleanupProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.properties.TasksExecutorPoolProperties;
import com.netflix.genie.web.properties.TasksSchedulerPoolProperties;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.FilePersistenceService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.TagPersistenceService;
import com.netflix.genie.web.tasks.leader.ClusterCheckerTask;
import com.netflix.genie.web.tasks.leader.DatabaseCleanupTask;
import com.netflix.genie.web.tasks.node.DiskCleanupTask;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

/**
 * Configuration of beans for asynchronous tasks within Genie.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(
    {
        ClusterCheckerProperties.class,
        DatabaseCleanupProperties.class,
        DiskCleanupProperties.class,
        TasksExecutorPoolProperties.class,
        TasksSchedulerPoolProperties.class
    }
)
public class GenieTasksAutoConfiguration {

    /**
     * Get an {@link Executor} to use for executing processes from tasks.
     *
     * @return The executor to use
     */
    @Bean
    @ConditionalOnMissingBean(Executor.class)
    public Executor processExecutor() {
        final Executor executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(null, null));
        return executor;
    }

    /**
     * Get a task scheduler.
     *
     * @param tasksSchedulerPoolProperties The properties regarding the thread pool to use for task scheduling
     * @return The task scheduler
     */
    @Bean
    @ConditionalOnMissingBean(name = "genieTaskScheduler")
    public ThreadPoolTaskScheduler genieTaskScheduler(
        final TasksSchedulerPoolProperties tasksSchedulerPoolProperties
    ) {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(tasksSchedulerPoolProperties.getSize());
        scheduler.setThreadNamePrefix(tasksSchedulerPoolProperties.getThreadNamePrefix());
        return scheduler;
    }

    /**
     * Get a task executor for executing tasks asynchronously that don't need to be scheduled at a recurring rate.
     *
     * @param tasksExecutorPoolProperties The properties for the task executor thread pool
     * @return The task executor the system to use
     */
    @Bean
    @ConditionalOnMissingBean(name = "genieAsyncTaskExecutor")
    public AsyncTaskExecutor genieAsyncTaskExecutor(final TasksExecutorPoolProperties tasksExecutorPoolProperties) {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(tasksExecutorPoolProperties.getSize());
        executor.setThreadNamePrefix(tasksExecutorPoolProperties.getThreadNamePrefix());
        return executor;
    }

    /**
     * Synchronous task executor.
     *
     * @return The synchronous task executor to use
     */
    @Bean
    @ConditionalOnMissingBean(name = "genieSyncTaskExecutor")
    public SyncTaskExecutor genieSyncTaskExecutor() {
        return new SyncTaskExecutor();
    }

    /**
     * Create a {@link ClusterCheckerTask} if one hasn't been supplied.
     *
     * @param genieHostInfo         Information about the host this Genie process is running on
     * @param properties            The properties to use to configure the task
     * @param jobSearchService      The job search service to use
     * @param jobPersistenceService The job persistence service to use
     * @param restTemplate          The rest template for http calls
     * @param webEndpointProperties The properties where Spring actuator is running
     * @param registry              The spectator registry for getting metrics
     * @return The {@link ClusterCheckerTask} instance
     */
    @Bean
    @ConditionalOnMissingBean(ClusterCheckerTask.class)
    public ClusterCheckerTask clusterCheckerTask(
        final GenieHostInfo genieHostInfo,
        final ClusterCheckerProperties properties,
        final JobSearchService jobSearchService,
        final JobPersistenceService jobPersistenceService,
        @Qualifier("genieRestTemplate") final RestTemplate restTemplate,
        final WebEndpointProperties webEndpointProperties,
        final MeterRegistry registry
    ) {
        return new ClusterCheckerTask(
            genieHostInfo,
            properties,
            jobSearchService,
            jobPersistenceService,
            restTemplate,
            webEndpointProperties,
            registry
        );
    }

    /**
     * Create a {@link DatabaseCleanupTask} if one is required.
     *
     * @param cleanupProperties         The properties to use to configure this task
     * @param jobPersistenceService     The persistence service to use to cleanup the data store
     * @param clusterPersistenceService The cluster service to use to delete terminated clusters
     * @param filePersistenceService    The file service to use to delete unused file references
     * @param tagPersistenceService     The tag service to use to delete unused tag references
     * @param registry                  The metrics registry
     * @return The {@link DatabaseCleanupTask} instance to use if the conditions match
     */
    @Bean
    @ConditionalOnProperty(value = DatabaseCleanupProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(DatabaseCleanupTask.class)
    public DatabaseCleanupTask databaseCleanupTask(
        final DatabaseCleanupProperties cleanupProperties,
        final JobPersistenceService jobPersistenceService,
        final ClusterPersistenceService clusterPersistenceService,
        final FilePersistenceService filePersistenceService,
        final TagPersistenceService tagPersistenceService,
        final MeterRegistry registry
    ) {
        return new DatabaseCleanupTask(
            cleanupProperties,
            jobPersistenceService,
            clusterPersistenceService,
            filePersistenceService,
            tagPersistenceService,
            registry
        );
    }

    /**
     * If required get a {@link DiskCleanupTask} instance for use.
     *
     * @param properties       The disk cleanup properties to use.
     * @param scheduler        The scheduler to use to schedule the cron trigger.
     * @param jobsDir          The resource representing the location of the job directory
     * @param jobSearchService The service to find jobs with
     * @param jobsProperties   The jobs properties to use
     * @param processExecutor  The process executor to use to delete directories
     * @param registry         The metrics registry
     * @return The {@link DiskCleanupTask} instance
     * @throws IOException When it is unable to open a file reference to the job directory
     */
    @Bean
    @ConditionalOnProperty(value = DiskCleanupProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(DiskCleanupTask.class)
    public DiskCleanupTask diskCleanupTask(
        final DiskCleanupProperties properties,
        @Qualifier("genieTaskScheduler") final TaskScheduler scheduler,
        final Resource jobsDir,
        final JobSearchService jobSearchService,
        final JobsProperties jobsProperties,
        final Executor processExecutor,
        final MeterRegistry registry
    ) throws IOException {
        return new DiskCleanupTask(
            properties,
            scheduler,
            jobsDir,
            jobSearchService,
            jobsProperties,
            processExecutor,
            registry
        );
    }
}
