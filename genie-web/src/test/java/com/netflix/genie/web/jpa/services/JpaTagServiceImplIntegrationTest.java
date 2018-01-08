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
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.services.TagService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;
import java.util.UUID;

/**
 * Integration tests for the JpaTagServiceImpl class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(IntegrationTest.class)
@DatabaseTearDown("cleanup.xml")
public class JpaTagServiceImplIntegrationTest extends DBUnitTestBase {

    // This needs to be injected as a Spring Bean otherwise transactions don't work as there is no proxy
    @Autowired
    private TagService tagService;

    @Autowired
    private JpaTagRepository tagRepository;

    /**
     * Make sure that no matter how many times we try to create a tag it doesn't throw an error on duplicate key it
     * just does nothing.
     *
     * @throws GenieException on error
     */
    @Test
    public void canCreateTagIfNotExists() throws GenieException {
        Assert.assertThat(this.tagRepository.count(), Matchers.is(0L));
        final String tag = UUID.randomUUID().toString();
        this.tagService.createTagIfNotExists(tag);
        Assert.assertThat(this.tagRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.tagRepository.existsByTag(tag));
        final Optional<TagEntity> tagEntityOptional = this.tagRepository.findByTag(tag);
        Assert.assertTrue(tagEntityOptional.isPresent());
        final TagEntity tagEntity = tagEntityOptional.get();
        Assert.assertThat(tagEntity.getTag(), Matchers.is(tag));

        // Try again with the same tag
        this.tagService.createTagIfNotExists(tag);
        Assert.assertThat(this.tagRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.tagRepository.existsByTag(tag));
        final Optional<TagEntity> tagEntityOptional2 = this.tagRepository.findByTag(tag);
        Assert.assertTrue(tagEntityOptional2.isPresent());
        final TagEntity tagEntity2 = tagEntityOptional2.get();
        Assert.assertThat(tagEntity2.getTag(), Matchers.is(tag));

        // Make sure the ids are still equal
        Assert.assertThat(tagEntity2.getId(), Matchers.is(tagEntity.getId()));
    }
}
