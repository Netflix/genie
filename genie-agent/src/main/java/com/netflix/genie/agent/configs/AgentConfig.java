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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Configuration for various agent beans.
 *
 * @author standon
 * @since 4.0.0
 */
@Configuration
class AgentConfig {

    /**
     * Get a task executor for cleaning up {@link com.netflix.genie.agent.execution.services.FetchingCacheService}.
     *
     * @return The task executor to use
     */
    @Bean
    @Lazy
    public TaskExecutor fetchingCacheServiceCleanUpTaskExecutor() {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        //TODO Pool size should be parametrized once its decide how agent properties will be configured
        executor.setCorePoolSize(1);
        return executor;
    }

}
