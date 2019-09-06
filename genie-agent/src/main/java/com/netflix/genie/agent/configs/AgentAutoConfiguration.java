/*
 *
 *  Copyright 2017 Netflix, Inc.
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

import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.agent.AgentMetadataImpl;
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Configuration for various agent beans.
 *
 * @author standon
 * @since 4.0.0
 */
@Configuration
public class AgentAutoConfiguration {

    /**
     * Provide a lazy bean definition for {@link AgentMetadata} if none already exists.
     *
     * @return A {@link AgentMetadataImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(AgentMetadata.class)
    public AgentMetadataImpl agentMetadata() {
        return new AgentMetadataImpl();
    }

    /**
     * Provide a lazy {@link FileLockFactory}.
     *
     * @return A {@link FileLockFactory} instance
     */
    @Bean
    @Lazy
    public FileLockFactory fileLockFactory() {
        return new FileLockFactory();
    }

    /**
     * Get a lazy {@link AsyncTaskExecutor} bean which may be shared by different components if one isn't already
     * defined.
     *
     * @return A {@link ThreadPoolTaskExecutor} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(name = "sharedAgentTaskExecutor", value = AsyncTaskExecutor.class)
    public AsyncTaskExecutor sharedAgentTaskExecutor() {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setThreadNamePrefix("agent-task-executor-");
        return executor;
    }

    /**
     * Provide a lazy {@link TaskScheduler} to be used by the Agent process if one isn't already defined.
     *
     * @return A {@link ThreadPoolTaskScheduler} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(name = "sharedAgentTaskScheduler")
    public TaskScheduler sharedAgentTaskScheduler() {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(5); // Big enough?
        scheduler.setThreadNamePrefix("agent-task-scheduler-");
        return scheduler;
    }

    /**
     * Provide a lazy {@link TaskScheduler} bean for use by the heart beat service is none has already been
     * defined in the context.
     *
     * @return A {@link TaskScheduler} that the heart beat service should use
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(name = "heartBeatServiceTaskScheduler")
    public TaskScheduler heartBeatServiceTaskScheduler() {
        final ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(1);
        taskScheduler.initialize();
        return taskScheduler;
    }
}
