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
package com.netflix.genie.web.data.entities;

import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

/**
 * Tests for the CriterionEntity class.
 *
 * @author tgianos
 * @since 3.3.0
 */
class CriterionEntityTest extends EntityTestBase {

    /**
     * Make sure the argument constructor sets the tags argument.
     */
    @Test
    void canCreateCriterionEntityWithTags() {
        CriterionEntity entity = new CriterionEntity(null, null, null, null, null);
        Assertions.assertThat(entity.getTags()).isEmpty();
        final Set<TagEntity> tags = Sets.newHashSet();
        entity = new CriterionEntity(null, null, null, null, tags);
        Assertions.assertThat(entity.getTags()).isEmpty();
        tags.add(new TagEntity(UUID.randomUUID().toString()));
        tags.add(new TagEntity(UUID.randomUUID().toString()));
        entity = new CriterionEntity(null, null, null, null, tags);
        Assertions.assertThat(entity.getTags()).isEqualTo(tags);
    }

    /**
     * Make sure we can create a criterion.
     */
    @Test
    void canCreateCriterionEntity() {
        final CriterionEntity entity = new CriterionEntity();
        Assertions.assertThat(entity.getUniqueId()).isNotPresent();
        Assertions.assertThat(entity.getName()).isNotPresent();
        Assertions.assertThat(entity.getVersion()).isNotPresent();
        Assertions.assertThat(entity.getStatus()).isNotPresent();
        Assertions.assertThat(entity.getTags()).isEmpty();
    }

    /**
     * Make sure setter is using the right field.
     */
    @Test
    void canSetUniqueId() {
        final CriterionEntity entity = new CriterionEntity();
        Assertions.assertThat(entity.getUniqueId()).isNotPresent();
        final String uniqueId = UUID.randomUUID().toString();
        entity.setUniqueId(uniqueId);
        Assertions.assertThat(entity.getUniqueId()).isPresent().contains(uniqueId);
    }

    /**
     * Make sure setter is using the right field.
     */
    @Test
    void canSetName() {
        final CriterionEntity entity = new CriterionEntity();
        Assertions.assertThat(entity.getName()).isNotPresent();
        final String name = UUID.randomUUID().toString();
        entity.setName(name);
        Assertions.assertThat(entity.getName()).isPresent().contains(name);
    }

    /**
     * Make sure setter is using the right field.
     */
    @Test
    void canSetVersion() {
        final CriterionEntity entity = new CriterionEntity();
        Assertions.assertThat(entity.getVersion()).isNotPresent();
        final String version = UUID.randomUUID().toString();
        entity.setVersion(version);
        Assertions.assertThat(entity.getVersion()).isPresent().contains(version);
    }

    /**
     * Make sure setter is using the right field.
     */
    @Test
    void canSetStatus() {
        final CriterionEntity entity = new CriterionEntity();
        Assertions.assertThat(entity.getStatus()).isNotPresent();
        final String status = UUID.randomUUID().toString();
        entity.setStatus(status);
        Assertions.assertThat(entity.getStatus()).isPresent().contains(status);
    }

    /**
     * Make sure setting the tags works.
     */
    @Test
    void canSetTags() {
        final CriterionEntity entity = new CriterionEntity();
        Assertions.assertThat(entity.getTags()).isEmpty();
        entity.setTags(null);
        Assertions.assertThat(entity.getTags()).isEmpty();
        final Set<TagEntity> tags = Sets.newHashSet();
        entity.setTags(tags);
        Assertions.assertThat(entity.getTags()).isEmpty();
        tags.add(new TagEntity(UUID.randomUUID().toString()));
        entity.setTags(tags);
        Assertions.assertThat(entity.getTags()).isEqualTo(tags);
    }

    /**
     * Test to make sure equals and hash code only care about the id of the base class not the tags.
     */
    @Test
    void testEqualsAndHashCode() {
        final Set<TagEntity> tags = Sets.newHashSet(
            new TagEntity(UUID.randomUUID().toString()),
            new TagEntity(UUID.randomUUID().toString())
        );
        final CriterionEntity one = new CriterionEntity(null, null, null, null, tags);
        final CriterionEntity two = new CriterionEntity(null, null, null, null, tags);
        final CriterionEntity three = new CriterionEntity();

        Assertions.assertThat(one).isEqualTo(two);
        Assertions.assertThat(one).isEqualTo(three);
        Assertions.assertThat(two).isEqualTo(three);

        Assertions.assertThat(one.hashCode()).isEqualTo(two.hashCode());
        Assertions.assertThat(one.hashCode()).isEqualTo(three.hashCode());
        Assertions.assertThat(two.hashCode()).isEqualTo(three.hashCode());
    }

    /**
     * Test the toString method.
     */
    @Test
    void testToString() {
        Assertions.assertThat(new CriterionEntity().toString()).isNotBlank();
    }
}
