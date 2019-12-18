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
 * Unit tests for the FileEntity class.
 *
 * @author tgianos
 * @since 3.3.0
 */
class FileEntityTest extends EntityTestBase {

    /**
     * Make sure the argument constructor sets the file argument.
     */
    @Test
    void canCreateFileEntityWithFile() {
        final String file = UUID.randomUUID().toString();
        final FileEntity fileEntity = new FileEntity(file);
        Assertions.assertThat(fileEntity.getFile()).isEqualTo(file);
    }

    /**
     * Make sure we can create a file.
     */
    @Test
    void canCreateFileEntity() {
        final FileEntity fileEntity = new FileEntity();
        final String file = UUID.randomUUID().toString();
        fileEntity.setFile(file);
        Assertions.assertThat(fileEntity.getFile()).isEqualTo(file);
    }

    /**
     * Make sure a file can't be validated if it exceeds size limitations.
     */
    @Test
    void cantCreateFileEntityDueToSize() {
        final FileEntity fileEntity = new FileEntity();
        final String file = StringUtils.rightPad(UUID.randomUUID().toString(), 1025);
        fileEntity.setFile(file);
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(fileEntity));
    }

    /**
     * Make sure a file can't be validated if the value is blank.
     */
    @Test
    void cantCreateFileEntityDueToNoFile() {
        final FileEntity fileEntity = new FileEntity();
        final String file = "";
        fileEntity.setFile(file);
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(fileEntity));
    }

    /**
     * Test to make sure equals and hash code only care about the unique file.
     */
    @Test
    void testEqualsAndHashCode() {
        final String file = UUID.randomUUID().toString();
        final FileEntity one = new FileEntity();
        one.setFile(file);
        final FileEntity two = new FileEntity();
        two.setFile(file);
        final FileEntity three = new FileEntity();
        three.setFile(UUID.randomUUID().toString());

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
        Assertions.assertThat(new FileEntity().toString()).isNotBlank();
    }
}
