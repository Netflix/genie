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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

/**
 * Unit tests for the BaseSearchResult class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class BaseSearchResultTest {

    /**
     * Make sure the constructor and getters work properly.
     */
    @Test
    void canConstruct() {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        final String user = UUID.randomUUID().toString();
        final BaseSearchResult searchResult = new BaseSearchResult(id, name, user);

        Assertions.assertThat(searchResult.getId()).isEqualTo(id);
        Assertions.assertThat(searchResult.getName()).isEqualTo(name);
        Assertions.assertThat(searchResult.getUser()).isEqualTo(user);
    }

    /**
     * Make sure the equals function only acts on id.
     */
    @Test
    void canFindEquality() {
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

        Assertions.assertThat(searchResult1).isEqualTo(searchResult2);
        Assertions.assertThat(searchResult1).isNotEqualTo(searchResult3);
        Assertions.assertThat(searchResult1.hashCode()).isEqualTo(searchResult2.hashCode());
        Assertions.assertThat(searchResult1.hashCode()).isNotEqualTo(searchResult3.hashCode());
    }

    /**
     * Test to make sure we can create a valid JSON string from a DTO object.
     */
    @Test
    void canCreateValidJsonString() {
        final BaseSearchResult searchResult = new BaseSearchResult(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final String json = searchResult.toString();
        Assertions.assertThatCode(() -> GenieObjectMapper.getMapper().readTree(json)).doesNotThrowAnyException();
    }
}
