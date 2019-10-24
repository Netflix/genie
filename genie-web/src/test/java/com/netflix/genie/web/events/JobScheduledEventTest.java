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
package com.netflix.genie.web.events;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Tests for the JobScheduledEvent class.
 */
class JobScheduledEventTest {

    /**
     * Make sure we can successfully create a Job Scheduled Event.
     */
    @Test
    void canConstruct() {
        final String jobId = UUID.randomUUID().toString();
        final Future<?> task = Mockito.mock(Future.class);
        final int memory = 1_034;
        final Object source = new Object();
        final JobScheduledEvent event = new JobScheduledEvent(jobId, task, memory, source);
        Assertions.assertThat(event.getId()).isEqualTo(jobId);
        Assertions.assertThat(event.getTask()).isEqualTo(task);
        Assertions.assertThat(event.getMemory()).isEqualTo(memory);
        Assertions.assertThat(event.getSource()).isEqualTo(source);
    }
}
