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
package com.netflix.genie.common.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.netflix.genie.common.exceptions.GenieException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit tests for the JsonUtils class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JsonUtilsTest {

    /**
     * Test the constructor.
     */
    @Test
    void canConstructInPackage() {
        Assertions.assertThat(new JsonUtils()).isNotNull();
    }

    /**
     * Test to make sure we can marshall an Object to a JSON string.
     *
     * @throws GenieException On marshalling error
     */
    @Test
    void canMarshall() throws GenieException {
        final List<String> strings = Lists.newArrayList("one", "two", "three");
        Assertions.assertThat(JsonUtils.marshall(strings)).isEqualTo("[\"one\",\"two\",\"three\"]");
    }

    /**
     * Test to make sure can successfully un-marshall a collection.
     *
     * @throws GenieException for any problems during the process
     */
    @Test
    void canUnmarshall() throws GenieException {
        final String source = "[\"one\",\"two\",\"three\"]";
        final TypeReference<List<String>> list = new TypeReference<List<String>>() {
        };

        Assertions
            .assertThat(JsonUtils.unmarshall(source, list))
            .isEqualTo(Lists.newArrayList("one", "two", "three"));
        Assertions.assertThat(JsonUtils.unmarshall(null, list)).isEqualTo(Lists.newArrayList());
    }
}
