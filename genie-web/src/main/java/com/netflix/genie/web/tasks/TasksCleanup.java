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
package com.netflix.genie.web.tasks;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;

/**
 * Performs any cleanup when the system is shutting down.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Component
public class TasksCleanup {

    private final ThreadPoolTaskScheduler scheduler;

    /**
     * Constructor.
     *
     * @param scheduler The task scheduler for the system.
     */
    @Autowired
    public TasksCleanup(@NotNull final ThreadPoolTaskScheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * When the spring context is about to close make sure we shutdown the thread pool and anything else we need to do.
     *
     * @param event The context closed event
     */
    @EventListener
    public void onShutdown(final ContextClosedEvent event) {
        this.scheduler.shutdown();
    }
}
