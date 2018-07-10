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

import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.properties.DiskCleanupProperties;
import com.netflix.genie.web.properties.TasksExecutorPoolProperties;
import com.netflix.genie.web.properties.TasksSchedulerPoolProperties;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

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
}
