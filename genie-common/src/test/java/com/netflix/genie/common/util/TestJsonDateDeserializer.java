/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Date;

/**
 * Test the TestJsonDateSerializer.
 *
 * @author tgianos
 */
public class TestJsonDateDeserializer {
    private static final String DATE_STRING = "2014-07-18T14:04:32.021-08:00";
    private static final long EXPECTED_MILLISECONDS = 1405721072021L;

    private JsonParser parser;
    private DeserializationContext context;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.parser = Mockito.mock(JsonParser.class);
        this.context = Mockito.mock(DeserializationContext.class);
    }

    /**
     * Test the de-serialization method.
     *
     * @throws IOException
     */
    @Test
    public void testDeserialize() throws IOException {
        Mockito.when(this.parser.getText()).thenReturn(DATE_STRING);

        final JsonDateDeserializer deserializer = new JsonDateDeserializer();
        final Date date = deserializer.deserialize(this.parser, this.context);
        Assert.assertEquals(EXPECTED_MILLISECONDS, date.getTime());
    }

    /**
     * Test the de-serialization method.
     *
     * @throws IOException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testDeserializeError() throws IOException {
        Mockito.when(parser.getText()).thenReturn("I am not a valid date.");

        final JsonDateDeserializer deserializer = new JsonDateDeserializer();
        final Date date = deserializer.deserialize(this.parser, this.context);
        Assert.assertEquals(EXPECTED_MILLISECONDS, date.getTime());
    }
}
