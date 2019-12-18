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

import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.validation.ConstraintViolationException;
import java.util.UUID;

/**
 * Unit tests for the TagEntity class.
 *
 * @author tgianos
 * @since 3.3.0
 */
class TagEntityTest extends EntityTestBase {

    /**
     * Make sure the argument constructor sets the tag argument.
     */
    @Test
    void canCreateTagEntityWithTag() {
        final String tag = UUID.randomUUID().toString();
        final TagEntity tagEntity = new TagEntity(tag);
        Assertions.assertThat(tagEntity.getTag()).isEqualTo(tag);
    }

    /**
     * Make sure we can create a tag.
     */
    @Test
    void canCreateTagEntity() {
        final TagEntity tagEntity = new TagEntity();
        final String tag = UUID.randomUUID().toString();
        tagEntity.setTag(tag);
        Assertions.assertThat(tagEntity.getTag()).isEqualTo(tag);
    }

    /**
     * Make sure a tag can't be validated if it exceeds size limitations.
     */
    @Test
    void cantCreateTagEntityDueToSize() {
        final TagEntity tagEntity = new TagEntity();
        final String tag = StringUtils.rightPad(UUID.randomUUID().toString(), 256);
        tagEntity.setTag(tag);
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(tagEntity));
    }

    /**
     * Make sure a tag can't be validated if the value is blank.
     */
    @Test
    void cantCreateTagEntityDueToNoTag() {
        final TagEntity tagEntity = new TagEntity();
        final String tag = "";
        tagEntity.setTag(tag);
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(tagEntity));
    }

    /**
     * Test to make sure equals and hash code only care about the unique tag.
     */
    @Test
    void testEqualsAndHashCode() {
        final String tag = UUID.randomUUID().toString();
        final TagEntity one = new TagEntity(tag);
        final TagEntity two = new TagEntity(tag);
        final TagEntity three = new TagEntity(UUID.randomUUID().toString());

        Assertions.assertThat(one).isEqualTo(two);
        Assertions.assertThat(one).isNotEqualTo(three);
        Assertions.assertThat(two).isNotEqualTo(three);

        Assertions.assertThat(one.hashCode()).isEqualTo(two.hashCode());
        Assertions.assertThat(one.hashCode()).isNotEqualTo(three.hashCode());
        Assertions.assertThat(two.hashCode()).isNotEqualTo(three.hashCode());
    }

    /**
     * Test the toString method.
     */
    @Test
    void testToString() {
        Assertions.assertThat(new TagEntity().toString()).isNotBlank();
    }
}
