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

import com.netflix.genie.test.categories.UnitTest;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.UUID;

/**
 * Unit tests for the TagEntity class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(UnitTest.class)
public class TagEntityUnitTest extends EntityTestsBase {

    /**
     * Make sure the argument constructor sets the tag argument.
     */
    @Test
    public void canCreateTagEntityWithTag() {
        final String tag = UUID.randomUUID().toString();
        final TagEntity tagEntity = new TagEntity(tag);
        Assert.assertThat(tagEntity.getTag(), Matchers.is(tag));
    }

    /**
     * Make sure we can create a tag.
     */
    @Test
    public void canCreateTagEntity() {
        final TagEntity tagEntity = new TagEntity();
        final String tag = UUID.randomUUID().toString();
        tagEntity.setTag(tag);
        Assert.assertThat(tagEntity.getTag(), Matchers.is(tag));
    }

    /**
     * Make sure a tag can't be validated if it exceeds size limitations.
     */
    @Test(expected = ConstraintViolationException.class)
    public void cantCreateTagEntityDueToSize() {
        final TagEntity tagEntity = new TagEntity();
        final String tag = StringUtils.rightPad(UUID.randomUUID().toString(), 256);
        tagEntity.setTag(tag);
        this.validate(tagEntity);
    }

    /**
     * Make sure a tag can't be validated if the value is blank.
     */
    @Test(expected = ConstraintViolationException.class)
    public void cantCreateTagEntityDueToNoTag() {
        final TagEntity tagEntity = new TagEntity();
        final String tag = null;
        tagEntity.setTag(tag);
        this.validate(tagEntity);
    }

    /**
     * Test to make sure equals and hash code only care about the unique tag.
     */
    @Test
    public void testEqualsAndHashCode() {
        final String tag = UUID.randomUUID().toString();
        final TagEntity one = new TagEntity();
        one.setTag(tag);
        final TagEntity two = new TagEntity();
        two.setTag(tag);
        final TagEntity three = new TagEntity();
        three.setTag(UUID.randomUUID().toString());

        Assert.assertTrue(one.equals(two));
        Assert.assertFalse(one.equals(three));
        Assert.assertFalse(two.equals(three));

        Assert.assertEquals(one.hashCode(), two.hashCode());
        Assert.assertNotEquals(one.hashCode(), three.hashCode());
        Assert.assertNotEquals(two.hashCode(), three.hashCode());
    }

    /**
     * Test the toString method.
     */
    @Test
    public void testToString() {
        Assert.assertNotNull(new TagEntity().toString());
    }
}
