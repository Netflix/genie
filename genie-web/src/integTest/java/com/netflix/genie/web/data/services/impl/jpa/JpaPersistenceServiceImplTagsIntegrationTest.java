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
package com.netflix.genie.web.data.services.impl.jpa;

import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.ApplicationMetadata;
import com.netflix.genie.common.external.dtos.v4.ApplicationRequest;
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

/**
 * Integration tests for the {@link JpaPersistenceServiceImpl} focusing on the Tag APIs.
 *
 * @author tgianos
 * @since 3.3.0
 */
class JpaPersistenceServiceImplTagsIntegrationTest extends JpaPersistenceServiceIntegrationTestBase {

    @Test
    void canDeleteUnusedTags() throws GenieCheckedException {
        Assertions.assertThat(this.tagRepository.count()).isEqualTo(0L);
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        this.tagRepository.saveAndFlush(new TagEntity(tag1));

        final ApplicationRequest app = new ApplicationRequest.Builder(
            new ApplicationMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ApplicationStatus.ACTIVE
            )
                .withTags(Sets.newHashSet(tag2))
                .build()
        ).build();

        // Create a relationship between tag2 and some resource in the system that should block it from being deleted
        this.service.saveApplication(app);

        Assertions.assertThat(this.tagRepository.existsByTag(tag1)).isTrue();
        Assertions.assertThat(this.tagRepository.existsByTag(tag2)).isTrue();

        Assertions.assertThat(this.service.deleteUnusedTags(Instant.now(), 10)).isEqualTo(1L);

        Assertions.assertThat(this.tagRepository.existsByTag(tag1)).isFalse();
        Assertions.assertThat(this.tagRepository.existsByTag(tag2)).isTrue();
    }
}
