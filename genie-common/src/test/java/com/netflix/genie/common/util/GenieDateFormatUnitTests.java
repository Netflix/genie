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
package com.netflix.genie.common.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.text.FieldPosition;
import java.util.Date;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for the GenieDateFormat class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieDateFormatUnitTests {

    private static final String EXPECTED_DATE_STRING = "1970-01-01T00:00:00.000Z";
    private static final Date DATE = new Date(0);
    // Limit random dates used in test between 'epoch' and 'epoch + 1000 years'
    private static final long MAX_RANDOM_DATE = TimeUnit.MILLISECONDS.convert(365 * 1000, TimeUnit.DAYS);

    /**
     * Make sure we can properly format a data in UTC time.
     */
    @Test
    public void canFormat() {
        final GenieDateFormat format = new GenieDateFormat();
        final StringBuffer buffer = new StringBuffer();
        final FieldPosition pos = Mockito.mock(FieldPosition.class);

        final String formattedDateString = format.format(DATE, buffer, pos).toString();
        Assert.assertThat(formattedDateString, Matchers.is(EXPECTED_DATE_STRING));
        Assert.assertTrue(GenieDateFormat.VALID_PATTERN.matcher(formattedDateString).matches());
    }

    /**
     * Generate random dates and test them against the expected format regex.
     */
    @Test
    public void formatMatchesExpected() {
        final GenieDateFormat format = new GenieDateFormat();
        final FieldPosition pos = Mockito.mock(FieldPosition.class);
        final Random random = new Random();

        for (int i = 0; i < 100; i++) {
            final Date date = new Date(random.nextLong() % MAX_RANDOM_DATE);
            final StringBuffer buffer = new StringBuffer();
            final String formattedDateString = format.format(date, buffer, pos).toString();
            Assert.assertTrue(
                "Failed to match: " + formattedDateString,
                GenieDateFormat.VALID_PATTERN.matcher(formattedDateString).matches());
        }
    }

    /**
     * Make sure OptionalDateJsonSerializer can serialize a valid date.
     *
     * @throws IOException if serialization fails
     */
    @Test
    public void canSerialize() throws IOException {
        final GenieDateFormat.OptionalDateJsonSerializer serializer = new GenieDateFormat.OptionalDateJsonSerializer();
        final JsonGenerator generatorMock = Mockito.mock(JsonGenerator.class);
        final SerializerProvider providerMock = Mockito.mock(SerializerProvider.class);

        serializer.serialize(
            Optional.of(DATE),
            generatorMock,
            providerMock
        );

        Mockito.verify(generatorMock).writeString(EXPECTED_DATE_STRING);
    }

    /**
     * Make sure OptionalDateJsonSerializer can serialize an empty Optional date.
     *
     * @throws IOException if serialization fails
     */
    @Test
    public void canSerializeEmpty() throws IOException {
        final GenieDateFormat.OptionalDateJsonSerializer serializer = new GenieDateFormat.OptionalDateJsonSerializer();
        final JsonGenerator generatorMock = Mockito.mock(JsonGenerator.class);
        final SerializerProvider providerMock = Mockito.mock(SerializerProvider.class);

        serializer.serialize(
            Optional.empty(),
            generatorMock,
            providerMock
        );

        Mockito.verify(generatorMock).writeNull();
    }
}
