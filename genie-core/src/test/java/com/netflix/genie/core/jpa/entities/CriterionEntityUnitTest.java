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
package com.netflix.genie.core.jpa.entities;

import com.google.common.collect.Sets;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
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
        CriterionEntity entity = new CriterionEntity(null);
        Assert.assertThat(entity.getTags(), Matchers.empty());
        final Set<TagEntity> tags = Sets.newHashSet();
        entity = new CriterionEntity(tags);
        Assert.assertThat(entity.getTags(), Matchers.empty());
        tags.add(new TagEntity(UUID.randomUUID().toString()));
        tags.add(new TagEntity(UUID.randomUUID().toString()));
        entity = new CriterionEntity(tags);
        Assert.assertThat(entity.getTags(), Matchers.is(tags));
    }

    /**
     * Make sure we can create a criterion.
     */
    @Test
    public void canCreateCriterionEntity() {
        final CriterionEntity entity = new CriterionEntity();
        Assert.assertThat(entity.getTags(), Matchers.empty());
    }

    /**
     * Make sure a criterion can't be validated if the tags are empty.
     */
    @Test(expected = ConstraintViolationException.class)
    public void cantSaveCriterionDueToNoTags() {
        final CriterionEntity entity = new CriterionEntity();
        this.validate(entity);
    }

    /**
     * Test to make sure equals and hash code only care about the unique file.
     */
    @Test
    public void testEqualsAndHashCode() {
        final Set<TagEntity> tags = Sets.newHashSet(
            new TagEntity(UUID.randomUUID().toString()),
            new TagEntity(UUID.randomUUID().toString())
        );
        final CriterionEntity one = new CriterionEntity(tags);
        final CriterionEntity two = new CriterionEntity(tags);
        final CriterionEntity three = new CriterionEntity();

        Assert.assertTrue(one.equals(two));
        Assert.assertFalse(one.equals(three));
        Assert.assertFalse(two.equals(three));

        Assert.assertEquals(one.hashCode(), two.hashCode());
        Assert.assertNotEquals(one.hashCode(), three.hashCode());
        Assert.assertNotEquals(two.hashCode(), three.hashCode());
    }
}
