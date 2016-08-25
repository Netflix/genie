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

import com.netflix.genie.web.tasks.leader.LeadershipTask;
import com.netflix.genie.web.tasks.leader.LeadershipTasksCoordinator;
import com.netflix.genie.web.tasks.leader.LocalLeader;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.Collection;

/**
 * Configuration of beans for asynchronous tasks within Genie.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
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

    /**
     * Create the LeadershipTasksCoordination bean used to start and stop all leadership related tasks based on
     * whether leadership is granted or revoked.
     *
     * @param taskScheduler The task scheduler to use for scheduling leadership tasks
     * @param tasks         The leadership tasks to schedule
     * @return The leader coordinator
     */
    @Bean
    @ConditionalOnBean(LeadershipTask.class)
    public LeadershipTasksCoordinator leadershipTasksCoordinator(
        final TaskScheduler taskScheduler,
        final Collection<LeadershipTask> tasks
    ) {
        return new LeadershipTasksCoordinator(taskScheduler, tasks);
    }

    /**
     * If Spring Cloud Leadership is disabled and this node is forced to be the leader create the local leader
     * bean which will fire appropriate events.
     *
     * @param publisher The application event publisher to use
     * @param isLeader  Whether this node is the leader of the cluster or not
     * @return The local leader bean
     */
    @Bean
    @ConditionalOnProperty(value = "spring.cloud.cluster.leader.enabled", havingValue = "false")
    public LocalLeader localLeader(
        final ApplicationEventPublisher publisher,
        @Value("${genie.leader.enabled}") final boolean isLeader
    ) {
        return new LocalLeader(publisher, isLeader);
    }
}
