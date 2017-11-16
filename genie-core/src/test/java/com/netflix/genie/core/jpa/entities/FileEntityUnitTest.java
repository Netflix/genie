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
 * Unit tests for the FileEntity class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(UnitTest.class)
public class FileEntityUnitTest extends EntityTestsBase {

    /**
     * Make sure the argument constructor sets the file argument.
     */
    @Test
    public void canCreateFileEntityWithFile() {
        final String file = UUID.randomUUID().toString();
        final FileEntity fileEntity = new FileEntity(file);
        Assert.assertThat(fileEntity.getFile(), Matchers.is(file));
    }

    /**
     * Make sure we can create a file.
     */
    @Test
    public void canCreateFileEntity() {
        final FileEntity fileEntity = new FileEntity();
        final String file = UUID.randomUUID().toString();
        fileEntity.setFile(file);
        Assert.assertThat(fileEntity.getFile(), Matchers.is(file));
    }

    /**
     * Make sure a file can't be validated if it exceeds size limitations.
     */
    @Test(expected = ConstraintViolationException.class)
    public void cantCreateFileEntityDueToSize() {
        final FileEntity fileEntity = new FileEntity();
        final String file = StringUtils.rightPad(UUID.randomUUID().toString(), 1025);
        fileEntity.setFile(file);
        this.validate(fileEntity);
    }

    /**
     * Make sure a file can't be validated if the value is blank.
     */
    @Test(expected = ConstraintViolationException.class)
    public void cantCreateFileEntityDueToNoFile() {
        final FileEntity fileEntity = new FileEntity();
        final String file = null;
        fileEntity.setFile(file);
        this.validate(fileEntity);
    }

    /**
     * Test to make sure equals and hash code only care about the unique file.
     */
    @Test
    public void testEqualsAndHashCode() {
        final String file = UUID.randomUUID().toString();
        final FileEntity one = new FileEntity();
        one.setFile(file);
        final FileEntity two = new FileEntity();
        two.setFile(file);
        final FileEntity three = new FileEntity();
        three.setFile(UUID.randomUUID().toString());

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
        Assert.assertNotNull(new FileEntity().toString());
    }
}
