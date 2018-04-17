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
package com.netflix.genie.web.jpa.entities;

import com.google.common.collect.Sets;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;
import java.util.UUID;

/**
 * Tests for the CriterionEntity class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(UnitTest.class)
public class CriterionEntityUnitTest extends EntityTestsBase {

    /**
     * Make sure the argument constructor sets the tags argument.
     */
    @Test
    public void canCreateCriterionEntityWithTags() {
        CriterionEntity entity = new CriterionEntity(null, null, null, null, null);
        Assert.assertThat(entity.getTags(), Matchers.empty());
        final Set<TagEntity> tags = Sets.newHashSet();
        entity = new CriterionEntity(null, null, null, null, tags);
        Assert.assertThat(entity.getTags(), Matchers.empty());
        tags.add(new TagEntity(UUID.randomUUID().toString()));
        tags.add(new TagEntity(UUID.randomUUID().toString()));
        entity = new CriterionEntity(null, null, null, null, tags);
        Assert.assertThat(entity.getTags(), Matchers.is(tags));
    }

    /**
     * Make sure we can create a criterion.
     */
    @Test
    public void canCreateCriterionEntity() {
        final CriterionEntity entity = new CriterionEntity();
        Assert.assertFalse(entity.getUniqueId().isPresent());
        Assert.assertFalse(entity.getName().isPresent());
        Assert.assertFalse(entity.getVersion().isPresent());
        Assert.assertFalse(entity.getStatus().isPresent());
        Assert.assertThat(entity.getTags(), Matchers.empty());
    }

    /**
     * Make sure setter is using the right field.
     */
    @Test
    public void canSetUniqueId() {
        final CriterionEntity entity = new CriterionEntity();
        Assert.assertFalse(entity.getUniqueId().isPresent());
        final String uniqueId = UUID.randomUUID().toString();
        entity.setUniqueId(uniqueId);
        Assert.assertThat(entity.getUniqueId().orElse(null), Matchers.is(uniqueId));
    }

    /**
     * Make sure setter is using the right field.
     */
    @Test
    public void canSetName() {
        final CriterionEntity entity = new CriterionEntity();
        Assert.assertFalse(entity.getName().isPresent());
        final String name = UUID.randomUUID().toString();
        entity.setName(name);
        Assert.assertThat(entity.getName().orElse(null), Matchers.is(name));
    }

    /**
     * Make sure setter is using the right field.
     */
    @Test
    public void canSetVersion() {
        final CriterionEntity entity = new CriterionEntity();
        Assert.assertFalse(entity.getVersion().isPresent());
        final String status = UUID.randomUUID().toString();
        entity.setVersion(status);
        Assert.assertThat(entity.getVersion().orElse(null), Matchers.is(status));
    }

    /**
     * Make sure setter is using the right field.
     */
    @Test
    public void canSetStatus() {
        final CriterionEntity entity = new CriterionEntity();
        Assert.assertFalse(entity.getStatus().isPresent());
        final String version = UUID.randomUUID().toString();
        entity.setStatus(version);
        Assert.assertThat(entity.getStatus().orElse(null), Matchers.is(version));
    }

    /**
     * Make sure setting the tags works.
     */
    @Test
    public void canSetTags() {
        final CriterionEntity entity = new CriterionEntity();
        Assert.assertTrue(entity.getTags().isEmpty());
        entity.setTags(null);
        Assert.assertTrue(entity.getTags().isEmpty());
        final Set<TagEntity> tags = Sets.newHashSet();
        entity.setTags(tags);
        Assert.assertTrue(entity.getTags().isEmpty());
        tags.add(new TagEntity(UUID.randomUUID().toString()));
        entity.setTags(tags);
        Assert.assertEquals(entity.getTags(), tags);
    }

    /**
     * Test to make sure equals and hash code only care about the id of the base class not the tags.
     */
    @Test
    public void testEqualsAndHashCode() {
        final Set<TagEntity> tags = Sets.newHashSet(
            new TagEntity(UUID.randomUUID().toString()),
            new TagEntity(UUID.randomUUID().toString())
        );
        final CriterionEntity one = new CriterionEntity(null, null, null, null, tags);
        final CriterionEntity two = new CriterionEntity(null, null, null, null, tags);
        final CriterionEntity three = new CriterionEntity();

        Assert.assertTrue(one.equals(two));
        Assert.assertTrue(one.equals(three));
        Assert.assertTrue(two.equals(three));

        Assert.assertEquals(one.hashCode(), two.hashCode());
        Assert.assertEquals(one.hashCode(), three.hashCode());
        Assert.assertEquals(two.hashCode(), three.hashCode());
    }

    /**
     * Test the toString method.
     */
    @Test
    public void testToString() {
        Assert.assertNotNull(new CriterionEntity().toString());
    }
}
