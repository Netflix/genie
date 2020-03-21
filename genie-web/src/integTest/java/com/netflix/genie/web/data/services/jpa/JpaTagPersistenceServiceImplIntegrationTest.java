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
package com.netflix.genie.web.data.services.jpa;

import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.external.dtos.v4.ApplicationMetadata;
import com.netflix.genie.common.external.dtos.v4.ApplicationRequest;
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus;
import com.netflix.genie.web.data.entities.TagEntity;
import com.netflix.genie.web.data.services.ApplicationPersistenceService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the {@link JpaTagPersistenceServiceImpl} class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@DatabaseTearDown("cleanup.xml")
class JpaTagPersistenceServiceImplIntegrationTest extends DBIntegrationTestBase {

    // This needs to be injected as a Spring Bean otherwise transactions don't work as there is no proxy
    @Autowired
    private JpaTagPersistenceServiceImpl tagPersistenceService;

    @Autowired
    private ApplicationPersistenceService applicationPersistenceService;

    /**
     * Make sure that no matter how many times we try to create a tag it doesn't throw an error on duplicate key it
     * just does nothing.
     */
    @Test
    void canCreateTagIfNotExists() {
        Assertions.assertThat(this.tagRepository.count()).isEqualTo(0L);
        final String tag = UUID.randomUUID().toString();
        Assertions.assertThat(this.tagPersistenceService.getTag(tag)).isNotPresent();
        this.tagPersistenceService.createTagIfNotExists(tag);
        Assertions.assertThat(this.tagRepository.count()).isEqualTo(1L);
        Assertions.assertThat(this.tagRepository.existsByTag(tag)).isTrue();
        Assertions
            .assertThat(this.tagPersistenceService.getTag(tag))
            .isPresent()
            .get()
            .extracting(TagEntity::getTag)
            .isEqualTo(tag);

        // Try again with the same tag
        this.tagPersistenceService.createTagIfNotExists(tag);
        Assertions.assertThat(this.tagRepository.count()).isEqualTo(1L);
        Assertions.assertThat(this.tagRepository.existsByTag(tag)).isTrue();
        Assertions
            .assertThat(this.tagPersistenceService.getTag(tag))
            .isPresent()
            .get()
            .extracting(TagEntity::getTag)
            .isEqualTo(tag);

        Assertions.assertThat(this.tagRepository.count()).isEqualTo(1L);
    }

    @Test
    void canDeleteUnusedTags() throws GenieException {
        Assertions.assertThat(this.tagRepository.count()).isEqualTo(0L);
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        this.tagPersistenceService.createTagIfNotExists(tag1);

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

        this.applicationPersistenceService.createApplication(app);

        Assertions.assertThat(this.tagRepository.existsByTag(tag1)).isTrue();
        Assertions.assertThat(this.tagRepository.existsByTag(tag2)).isTrue();

        Assertions.assertThat(this.tagPersistenceService.deleteUnusedTags(Instant.now())).isEqualTo(1L);

        Assertions.assertThat(this.tagRepository.existsByTag(tag1)).isFalse();
        Assertions.assertThat(this.tagRepository.existsByTag(tag2)).isTrue();
    }

    @Test
    void canFindTags() {
        Assertions.assertThat(this.tagRepository.count()).isEqualTo(0L);
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        this.tagPersistenceService.createTagIfNotExists(tag1);
        final TagEntity tagEntity1 = this.tagPersistenceService.getTag(tag1).orElseThrow(IllegalArgumentException::new);
        this.tagPersistenceService.createTagIfNotExists(tag2);
        final TagEntity tagEntity2 = this.tagPersistenceService.getTag(tag2).orElseThrow(IllegalArgumentException::new);

        Set<TagEntity> tags = this.tagPersistenceService.getTags(Sets.newHashSet(tag1, tag2));
        Assertions.assertThat(tags).hasSize(2).contains(tagEntity1, tagEntity2);

        tags = this.tagPersistenceService.getTags(Sets.newHashSet(tag1, tag2, UUID.randomUUID().toString()));
        Assertions.assertThat(tags).hasSize(2).contains(tagEntity1, tagEntity2);

        tags = this.tagPersistenceService.getTags(Sets.newHashSet(tag1, UUID.randomUUID().toString()));
        Assertions.assertThat(tags).hasSize(1).contains(tagEntity1);
    }
}
