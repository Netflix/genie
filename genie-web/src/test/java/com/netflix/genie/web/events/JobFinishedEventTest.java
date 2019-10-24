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

import java.util.UUID;

/**
 * Tests for the JobFinishedEvent class.
 */
class JobFinishedEventTest {

    /**
     * Make sure we can successfully create a Job Finished Event.
     */
    @Test
    void canConstruct() {
        final String id = UUID.randomUUID().toString();
        final JobFinishedReason reason = JobFinishedReason.PROCESS_COMPLETED;
        final String message = UUID.randomUUID().toString();
        final Object source = new Object();
        final JobFinishedEvent event = new JobFinishedEvent(id, reason, message, source);
        Assertions.assertThat(event).isNotNull();
        Assertions.assertThat(event.getId()).isEqualTo(id);
        Assertions.assertThat(event.getReason()).isEqualTo(reason);
        Assertions.assertThat(event.getMessage()).isEqualTo(message);
        Assertions.assertThat(event.getSource()).isEqualTo(source);
    }
}
