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
 * Unit tests for the kill job event.
 *
 * @author tgianos
 * @since 3.0.0
 */
class KillJobEventTest {

    /**
     * Make sure can get the Job Id back from the event.
     */
    @Test
    void canGetJobId() {
        final String id = UUID.randomUUID().toString();
        final String reason = UUID.randomUUID().toString();
        final KillJobEvent event = new KillJobEvent(id, reason, this);

        Assertions.assertThat(event.getId()).isEqualTo(id);
        Assertions.assertThat(event.getReason()).isEqualTo(reason);
    }
}
