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

import com.netflix.genie.common.util.GenieObjectMapper;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * Unit tests for the BaseSearchResult class.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class BaseSearchResultTest {

    /**
     * Make sure the constructor and getters work properly.
     */
    @Test
    public void canConstruct() {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        final String user = UUID.randomUUID().toString();
        final BaseSearchResult searchResult = new BaseSearchResult(id, name, user);

        Assert.assertThat(searchResult.getId(), Matchers.is(id));
        Assert.assertThat(searchResult.getName(), Matchers.is(name));
        Assert.assertThat(searchResult.getUser(), Matchers.is(user));
    }

    /**
     * Make sure the equals function only acts on id.
     */
    @Test
    public void canFindEquality() {
        final String id = UUID.randomUUID().toString();
        final BaseSearchResult searchResult1
            = new BaseSearchResult(id, UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final BaseSearchResult searchResult2
            = new BaseSearchResult(id, UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final BaseSearchResult searchResult3 = new BaseSearchResult(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        Assert.assertEquals(searchResult1, searchResult2);
        Assert.assertNotEquals(searchResult1, searchResult3);
    }

    /**
     * Make sure the hash code function only acts on id.
     */
    @Test
    public void canUseHashCode() {
        final String id = UUID.randomUUID().toString();
        final BaseSearchResult searchResult1
            = new BaseSearchResult(id, UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final BaseSearchResult searchResult2
            = new BaseSearchResult(id, UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final BaseSearchResult searchResult3 = new BaseSearchResult(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        Assert.assertEquals(searchResult1.hashCode(), searchResult2.hashCode());
        Assert.assertNotEquals(searchResult1.hashCode(), searchResult3.hashCode());
    }

    /**
     * Test to make sure we can create a valid JSON string from a DTO object.
     */
    @Test
    public void canCreateValidJsonString() {
        final BaseSearchResult searchResult = new BaseSearchResult(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final String json = searchResult.toString();
        try {
            GenieObjectMapper.getMapper().readTree(json);
        } catch (final IOException ioe) {
            Assert.fail();
        }
    }
}
