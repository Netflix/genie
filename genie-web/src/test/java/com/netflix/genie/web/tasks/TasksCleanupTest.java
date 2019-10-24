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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Tests the {@link TasksCleanup} class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class TasksCleanupTest {

    private ThreadPoolTaskScheduler scheduler;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.scheduler = Mockito.mock(ThreadPoolTaskScheduler.class);
    }

    /**
     * Make sure the thread pool scheduler is shutdown.
     */
    @Test
    void canShutdown() {
        final ContextClosedEvent event = Mockito.mock(ContextClosedEvent.class);
        final TasksCleanup cleanup = new TasksCleanup(this.scheduler);
        cleanup.onShutdown(event);
        Mockito.verify(this.scheduler, Mockito.times(1)).shutdown();
    }
}
