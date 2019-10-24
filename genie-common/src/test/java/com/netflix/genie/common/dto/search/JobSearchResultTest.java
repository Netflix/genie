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
package com.netflix.genie.common.dto.search;

import com.netflix.genie.common.dto.JobStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Tests for the {@link JobSearchResult} DTO.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobSearchResultTest {

    /**
     * Make sure constructor works.
     */
    @Test
    void canConstruct() {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        final String user = UUID.randomUUID().toString();
        final JobStatus status = JobStatus.FAILED;
        final Instant started = Instant.now();
        final Instant finished = Instant.now();
        final String clusterName = UUID.randomUUID().toString();
        final String commandName = UUID.randomUUID().toString();
        final JobSearchResult searchResult
            = new JobSearchResult(id, name, user, status, started, finished, clusterName, commandName);

        Assertions.assertThat(searchResult.getId()).isEqualTo(id);
        Assertions.assertThat(searchResult.getName()).isEqualTo(name);
        Assertions.assertThat(searchResult.getUser()).isEqualTo(user);
        Assertions.assertThat(searchResult.getStatus()).isEqualTo(status);
        Assertions.assertThat(searchResult.getStarted().orElseThrow(IllegalArgumentException::new)).isEqualTo(started);
        Assertions
            .assertThat(searchResult.getFinished().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(finished);
        Assertions
            .assertThat(searchResult.getClusterName().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(clusterName);
        Assertions
            .assertThat(searchResult.getCommandName().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(commandName);
        Assertions
            .assertThat(searchResult.getRuntime())
            .isEqualByComparingTo(Duration.ofMillis(finished.toEpochMilli() - started.toEpochMilli()));

        final JobSearchResult searchResult2 = new JobSearchResult(id, name, user, status, null, null, null, null);

        Assertions.assertThat(searchResult2.getId()).isEqualTo(id);
        Assertions.assertThat(searchResult2.getName()).isEqualTo(name);
        Assertions.assertThat(searchResult.getUser()).isEqualTo(user);
        Assertions.assertThat(searchResult2.getStatus()).isEqualTo(status);
        Assertions.assertThat(searchResult2.getStarted().isPresent()).isFalse();
        Assertions.assertThat(searchResult2.getFinished().isPresent()).isFalse();
        Assertions.assertThat(searchResult2.getClusterName().isPresent()).isFalse();
        Assertions.assertThat(searchResult2.getCommandName().isPresent()).isFalse();
        Assertions.assertThat(searchResult2.getRuntime()).isEqualTo(Duration.ZERO);
    }
}
