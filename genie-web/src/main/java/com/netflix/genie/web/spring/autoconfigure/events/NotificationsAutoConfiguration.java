/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.spring.autoconfigure.events;


import com.netflix.genie.web.data.observers.PersistedJobStatusObserver;
import com.netflix.genie.web.data.observers.PersistedJobStatusObserverImpl;
import com.netflix.genie.web.events.GenieEventBus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Beans related to external notifications.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
public class NotificationsAutoConfiguration {

    /**
     * Create {@link PersistedJobStatusObserver} if one does not exist.
     *
     * @param genieEventBus the genie event bus
     * @return a {@link PersistedJobStatusObserver}
     */
    @Bean
    @ConditionalOnMissingBean(PersistedJobStatusObserver.class)
    public PersistedJobStatusObserver persistedJobStatusObserver(
        final GenieEventBus genieEventBus
    ) {
        return new PersistedJobStatusObserverImpl(genieEventBus);
    }
}
