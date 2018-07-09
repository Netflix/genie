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
package com.netflix.genie.web.jpa.services;

import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dto.v4.ApplicationMetadata;
import com.netflix.genie.common.internal.dto.v4.ApplicationRequest;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the JpaTagPersistenceServiceImpl class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(IntegrationTest.class)
@DatabaseTearDown("cleanup.xml")
public class JpaTagPersistenceServiceImplIntegrationTest extends DBIntegrationTestBase {

    // This needs to be injected as a Spring Bean otherwise transactions don't work as there is no proxy
    @Autowired
    private JpaTagPersistenceService tagPersistenceService;

    @Autowired
    private ApplicationPersistenceService applicationPersistenceService;

    /**
     * Make sure that no matter how many times we try to create a tag it doesn't throw an error on duplicate key it
     * just does nothing.
     */
    @Test
    public void canCreateTagIfNotExists() {
        Assert.assertThat(this.tagRepository.count(), Matchers.is(0L));
        final String tag = UUID.randomUUID().toString();
        Assert.assertFalse(this.tagPersistenceService.getTag(tag).isPresent());
        this.tagPersistenceService.createTagIfNotExists(tag);
        Assert.assertThat(this.tagRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.tagRepository.existsByTag(tag));
        final Optional<TagEntity> tagEntityOptional = this.tagPersistenceService.getTag(tag);
        Assert.assertTrue(tagEntityOptional.isPresent());
        final TagEntity tagEntity = tagEntityOptional.get();
        Assert.assertThat(tagEntity.getTag(), Matchers.is(tag));

        // Try again with the same tag
        this.tagPersistenceService.createTagIfNotExists(tag);
        Assert.assertThat(this.tagRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.tagRepository.existsByTag(tag));
        final Optional<TagEntity> tagEntityOptional2 = this.tagPersistenceService.getTag(tag);
        Assert.assertTrue(tagEntityOptional2.isPresent());
        final TagEntity tagEntity2 = tagEntityOptional2.get();
        Assert.assertThat(tagEntity2.getTag(), Matchers.is(tag));

        // Make sure the ids are still equal
        Assert.assertThat(tagEntity2.getId(), Matchers.is(tagEntity.getId()));
    }

    /**
     * Make sure we can delete tags that aren't attached to other resources.
     *
     * @throws GenieException on error
     */
    @Test
    public void canDeleteUnusedTags() throws GenieException {
        Assert.assertThat(this.tagRepository.count(), Matchers.is(0L));
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

        Assert.assertTrue(this.tagRepository.existsByTag(tag1));
        Assert.assertTrue(this.tagRepository.existsByTag(tag2));

        Assert.assertThat(this.tagPersistenceService.deleteUnusedTags(Instant.now()), Matchers.is(1L));

        Assert.assertFalse(this.tagRepository.existsByTag(tag1));
        Assert.assertTrue(this.tagRepository.existsByTag(tag2));
    }

    /**
     * Make sure we can find tags.
     */
    @Test
    public void canFindTags() {
        Assert.assertThat(this.tagRepository.count(), Matchers.is(0L));
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        this.tagPersistenceService.createTagIfNotExists(tag1);
        final TagEntity tagEntity1 = this.tagPersistenceService.getTag(tag1).orElseThrow(IllegalArgumentException::new);
        this.tagPersistenceService.createTagIfNotExists(tag2);
        final TagEntity tagEntity2 = this.tagPersistenceService.getTag(tag2).orElseThrow(IllegalArgumentException::new);

        Set<TagEntity> tags = this.tagPersistenceService.getTags(Sets.newHashSet(tag1, tag2));
        Assert.assertThat(tags.size(), Matchers.is(2));
        Assert.assertThat(tags, Matchers.hasItems(tagEntity1, tagEntity2));

        tags = this.tagPersistenceService.getTags(Sets.newHashSet(tag1, tag2, UUID.randomUUID().toString()));
        Assert.assertThat(tags.size(), Matchers.is(2));
        Assert.assertThat(tags, Matchers.hasItems(tagEntity1, tagEntity2));

        tags = this.tagPersistenceService.getTags(Sets.newHashSet(tag1, UUID.randomUUID().toString()));
        Assert.assertThat(tags.size(), Matchers.is(1));
        Assert.assertThat(tags, Matchers.hasItem(tagEntity1));
    }
}
