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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Date;

/**
 * Test the TestJsonDateSerializer.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(UnitTest.class)
public class JsonDateSerializerUnitTests {
    private static final long MILLISECONDS = 1405715627311L;
    private static final Date DATE = new Date(MILLISECONDS);

    private String expectedString = "2014-07-18T20:33:47.311Z";

    /**
     * Test the serialization method.
     *
     * @throws IOException When exception happens during serialization
     */
    @Test
    public void testSerialize() throws IOException {
        final JsonGenerator gen = Mockito.mock(JsonGenerator.class);
        final SerializerProvider provider = Mockito.mock(SerializerProvider.class);

        final JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(DATE, gen, provider);
        Mockito.verify(gen, Mockito.times(1)).writeString(this.expectedString);
    }

    /**
     * Test the serialization method with a null date.
     *
     * @throws IOException When exception happens during serialization
     */
    @Test
    public void testSerializeNull() throws IOException {
        final JsonGenerator gen = Mockito.mock(JsonGenerator.class);
        final SerializerProvider provider = Mockito.mock(SerializerProvider.class);

        final JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(null, gen, provider);
        Mockito.verify(gen, Mockito.times(1)).writeString((String) null);
    }
}
