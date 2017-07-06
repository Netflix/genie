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

import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
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
public class TaskConfig {

    /**
     * Get an {@link Executor} to use for executing processes from tasks.
     *
     * @return The executor to use
     */
    @Bean
    public Executor processExecutor() {
        final Executor executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(null, null));
        return executor;
    }

    /**
     * A multicast (async) event publisher to replace the synchronous one used by Spring via the ApplicationContext.
     *
     * @param taskExecutor The task executor to use
     * @return The application event multicaster to use
     */
    @Bean
    public ApplicationEventMulticaster applicationEventMulticaster(final TaskExecutor taskExecutor) {
        final SimpleApplicationEventMulticaster applicationEventMulticaster = new SimpleApplicationEventMulticaster();
        applicationEventMulticaster.setTaskExecutor(taskExecutor);
        return applicationEventMulticaster;
    }

    /**
     * Get a task scheduler.
     *
     * @param poolSize The initial size of the thread pool that should be allocated
     * @return The task scheduler
     */
    @Bean
    public TaskScheduler taskScheduler(@Value("${genie.tasks.scheduler.pool.size:1}") final int poolSize) {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(poolSize);
        return scheduler;
    }

    /**
     * Get a task executor for executing tasks asynchronously that don't need to be scheduled at a recurring rate.
     *
     * @param poolSize The number of threads desired for this system. Likely best to do one more than number of CPUs
     * @return The task executor the system to use
     */
    @Bean
    public AsyncTaskExecutor taskExecutor(@Value("${genie.tasks.executor.pool.size:1}") final int poolSize) {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        return executor;
    }
}
