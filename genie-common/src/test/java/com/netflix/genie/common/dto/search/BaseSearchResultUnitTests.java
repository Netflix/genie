/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.common.dto.search;

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

/**
 * Unit tests for the BaseSearchResult class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class BaseSearchResultUnitTests {

    /**
     * Make sure the constructor and getters work properly.
     */
    @Test
    public void canConstruct() {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        final BaseSearchResult searchResult = new BaseSearchResult(id, name);

        Assert.assertThat(searchResult.getId(), Matchers.is(id));
        Assert.assertThat(searchResult.getName(), Matchers.is(name));
    }

    /**
     * Make sure the equals function only acts on id.
     */
    @Test
    public void canFindEquality() {
        final String id = UUID.randomUUID().toString();
        final BaseSearchResult searchResult1 = new BaseSearchResult(id, UUID.randomUUID().toString());
        final BaseSearchResult searchResult2 = new BaseSearchResult(id, UUID.randomUUID().toString());
        final BaseSearchResult searchResult3
            = new BaseSearchResult(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        Assert.assertTrue(searchResult1.equals(searchResult2));
        Assert.assertFalse(searchResult1.equals(searchResult3));
    }

    /**
     * Make sure the hash code function only acts on id.
     */
    @Test
    public void canUseHashCode() {
        final String id = UUID.randomUUID().toString();
        final BaseSearchResult searchResult1 = new BaseSearchResult(id, UUID.randomUUID().toString());
        final BaseSearchResult searchResult2 = new BaseSearchResult(id, UUID.randomUUID().toString());
        final BaseSearchResult searchResult3
            = new BaseSearchResult(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        Assert.assertEquals(searchResult1.hashCode(), searchResult2.hashCode());
        Assert.assertNotEquals(searchResult1.hashCode(), searchResult3.hashCode());
    }
}
