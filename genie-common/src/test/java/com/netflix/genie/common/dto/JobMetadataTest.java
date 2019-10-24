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
package com.netflix.genie.common.dto;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

/**
 * Unit tests for the JobMetadata class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobMetadataTest {

    /**
     * Test to make sure we can successfully build a JobMetadata class.
     */
    @Test
    void canBuild() {
        final String clientHost = UUID.randomUUID().toString();
        final String userAgent = UUID.randomUUID().toString();
        final int numAttachments = 38;
        final long totalSizeOfAttachments = 3809234L;
        final long stdOutSize = 80283L;
        final long stdErrSize = 8002343L;

        final JobMetadata metadata = new JobMetadata
            .Builder()
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments)
            .withStdOutSize(stdOutSize)
            .withStdErrSize(stdErrSize)
            .build();

        Assertions
            .assertThat(metadata.getClientHost().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(clientHost);
        Assertions.assertThat(metadata.getUserAgent().orElseThrow(IllegalArgumentException::new)).isEqualTo(userAgent);
        Assertions
            .assertThat(metadata.getNumAttachments().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(numAttachments);
        Assertions
            .assertThat(metadata.getTotalSizeOfAttachments().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(totalSizeOfAttachments);
        Assertions
            .assertThat(metadata.getStdOutSize().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(stdOutSize);
        Assertions
            .assertThat(metadata.getStdErrSize().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(stdErrSize);
    }

    /**
     * Test to make sure we can successfully find equality.
     */
    @Test
    void canFindEquality() {
        final String clientHost = UUID.randomUUID().toString();
        final String userAgent = UUID.randomUUID().toString();
        final int numAttachments = 38;
        final long totalSizeOfAttachments = 3809234L;

        final JobMetadata.Builder builder = new JobMetadata
            .Builder()
            .withId(UUID.randomUUID().toString())
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments);

        final JobMetadata one = builder.build();
        final JobMetadata two = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobMetadata three = builder.build();

        Assertions.assertThat(one).isEqualTo(two);
        Assertions.assertThat(one).isNotEqualTo(three);
        Assertions.assertThat(one).isNotEqualTo(new Object());
    }

    /**
     * Test to make sure we can successfully find equality.
     */
    @Test
    void canUseHashCode() {
        final String clientHost = UUID.randomUUID().toString();
        final String userAgent = UUID.randomUUID().toString();
        final int numAttachments = 38;
        final long totalSizeOfAttachments = 3809234L;

        final JobMetadata.Builder builder = new JobMetadata
            .Builder()
            .withId(UUID.randomUUID().toString())
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments);

        final JobMetadata one = builder.build();
        final JobMetadata two = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobMetadata three = builder.build();

        Assertions.assertThat(one.hashCode()).isEqualTo(two.hashCode());
        Assertions.assertThat(one.hashCode()).isNotEqualTo(three.hashCode());
    }
}
