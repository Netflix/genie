/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.web.jobs;

import com.netflix.genie.common.external.util.GenieObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Unit tests for the JobKillReasonFile class.
 *
 * @author mprimi
 * @since 3.0.8
 */
class JobKillReasonFileTest {

    private static final String KILL_REASON_STRING = "Test";

    @Test
    void serializeThenLoad() throws IOException {
        final JobKillReasonFile originalJobKillReasonFile = new JobKillReasonFile(KILL_REASON_STRING);

        Assertions.assertThat(originalJobKillReasonFile.getKillReason()).isEqualTo(KILL_REASON_STRING);

        final byte[] bytes = GenieObjectMapper.getMapper().writeValueAsBytes(originalJobKillReasonFile);

        final JobKillReasonFile loadedJobKillReasonFile
            = GenieObjectMapper.getMapper().readValue(bytes, JobKillReasonFile.class);

        Assertions.assertThat(loadedJobKillReasonFile.getKillReason()).isEqualTo(KILL_REASON_STRING);
    }
}
