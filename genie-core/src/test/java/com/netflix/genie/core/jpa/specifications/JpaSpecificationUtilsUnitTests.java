/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.jpa.specifications;

import com.google.common.collect.Sets;
import com.netflix.genie.core.jpa.entities.CommonFieldsEntity;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit tests for JpaSpecificationUtils.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JpaSpecificationUtilsUnitTests {

    /**
     * Utility class should have protected constructor.
     */
    @Test
    public void canAccessProtectedConstructor() {
        final JpaSpecificationUtils utils = new JpaSpecificationUtils();
        Assert.assertThat(utils, Matchers.notNullValue());
    }

    /**
     * Make sure we can get a valid like string for the tag list.
     */
    @Test
    public void canGetTagLikeString() {
        Assert.assertThat(JpaSpecificationUtils.getTagLikeString(Sets.newHashSet()), Matchers.is("%"));
        Assert.assertThat(
            JpaSpecificationUtils.getTagLikeString(Sets.newHashSet("tag")),
            Matchers.is("%" + CommonFieldsEntity.TAG_DELIMITER + "tag" + CommonFieldsEntity.TAG_DELIMITER + "%")
        );
        Assert.assertThat(
            JpaSpecificationUtils.getTagLikeString(Sets.newHashSet("tag", "Stag", "rag")),
            Matchers.is(
                "%"
                    + CommonFieldsEntity.TAG_DELIMITER
                    + "rag"
                    + CommonFieldsEntity.TAG_DELIMITER
                    + "%"
                    + CommonFieldsEntity.TAG_DELIMITER
                    + "Stag"
                    + CommonFieldsEntity.TAG_DELIMITER
                    + "%"
                    + CommonFieldsEntity.TAG_DELIMITER
                    + "tag"
                    + CommonFieldsEntity.TAG_DELIMITER
                    + "%"
            )
        );
    }
}
