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
package com.netflix.genie.web.configs;

import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.GenieEventBusImpl;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;

/**
 * Configuration related to Eventing within the Genie application.
 *
 * @author tgianos
 * @since 3.1.2
 */
@Configuration
@AutoConfigureAfter(GenieTasksAutoConfiguration.class)
public class GenieEventBusAutoConfiguration {

    /**
     * A multicast event publisher to replace the default one used by Spring via the ApplicationContext.
     *
     * @param syncTaskExecutor  The synchronous task executor to use
     * @param asyncTaskExecutor The asynchronous task executor to use
     * @return The application event multicaster to use
     */
    @Bean
    @ConditionalOnMissingBean(GenieEventBus.class)
    public GenieEventBusImpl applicationEventMulticaster(
        @Qualifier("genieSyncTaskExecutor") final SyncTaskExecutor syncTaskExecutor,
        @Qualifier("genieAsyncTaskExecutor") final AsyncTaskExecutor asyncTaskExecutor
    ) {
        final SimpleApplicationEventMulticaster syncMulticaster = new SimpleApplicationEventMulticaster();
        syncMulticaster.setTaskExecutor(syncTaskExecutor);

        final SimpleApplicationEventMulticaster asyncMulticaster = new SimpleApplicationEventMulticaster();
        asyncMulticaster.setTaskExecutor(asyncTaskExecutor);
        return new GenieEventBusImpl(syncMulticaster, asyncMulticaster);
    }
}
