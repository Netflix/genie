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
package com.netflix.genie.agent.spring.autoconfigure;

import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.agent.AgentMetadataImpl;
import com.netflix.genie.agent.properties.AgentProperties;
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.common.internal.util.HostnameUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.net.UnknownHostException;

/**
 * Configuration class for setting up agent-related beans.
 */
@Configuration
@EnableConfigurationProperties(AgentProperties.class)
public class AgentAutoConfiguration {

    /**
     * Provides the GenieHostInfo bean.
     *
     * @return a GenieHostInfo object containing the hostname
     * @throws UnknownHostException if the hostname cannot be determined
     */
    @Bean
    @ConditionalOnMissingBean(GenieHostInfo.class)
    public GenieHostInfo genieAgentHostInfo() throws UnknownHostException {
        final String hostname = HostnameUtil.getHostname();
        if (StringUtils.isBlank(hostname)) {
            throw new IllegalStateException("Unable to determine hostname.");
        }
        return new GenieHostInfo(hostname);
    }

    /**
     * Provides the AgentMetadata bean.
     *
     * @param genieHostInfo the GenieHostInfo containing the hostname
     * @return an instance of AgentMetadataImpl
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(AgentMetadata.class)
    public AgentMetadataImpl agentMetadata(final GenieHostInfo genieHostInfo) {
        return new AgentMetadataImpl(genieHostInfo.getHostname());
    }

    /**
     * Provides the FileLockFactory bean.
     *
     * @return a new instance of FileLockFactory
     */
    @Bean
    @Lazy
    public FileLockFactory fileLockFactory() {
        return new FileLockFactory();
    }

    /**
     * Provides a shared AsyncTaskExecutor bean.
     *
     * @param agentProperties the properties for configuring the agent
     * @return a configured ThreadPoolTaskExecutor
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(name = "sharedAgentTaskExecutor", value = AsyncTaskExecutor.class)
    public AsyncTaskExecutor sharedAgentTaskExecutor(final AgentProperties agentProperties) {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setThreadNamePrefix("agent-task-executor-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(
            (int) agentProperties.getShutdown().getInternalExecutorsLeeway().getSeconds()
        );
        executor.initialize();
        return executor;
    }

    /**
     * Provides a shared TaskScheduler bean.
     *
     * @param agentProperties the properties for configuring the agent
     * @return a configured ThreadPoolTaskScheduler
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(name = "sharedAgentTaskScheduler")
    public TaskScheduler sharedAgentTaskScheduler(final AgentProperties agentProperties) {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(5);
        scheduler.setThreadNamePrefix("agent-task-scheduler-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(
            (int) agentProperties.getShutdown().getInternalSchedulersLeeway().getSeconds()
        );
        scheduler.initialize();
        return scheduler;
    }

    /**
     * Provides a TaskScheduler bean for the heart beat service.
     *
     * @param agentProperties the properties for configuring the agent
     * @return a configured ThreadPoolTaskScheduler for heart beat service
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(name = "heartBeatServiceTaskScheduler")
    public TaskScheduler heartBeatServiceTaskScheduler(final AgentProperties agentProperties) {
        final ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(1);
        taskScheduler.setThreadNamePrefix("heart-beat-scheduler-");
        taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        taskScheduler.setAwaitTerminationSeconds(
            (int) agentProperties.getShutdown().getInternalSchedulersLeeway().getSeconds()
        );
        taskScheduler.initialize();
        return taskScheduler;
    }
}
