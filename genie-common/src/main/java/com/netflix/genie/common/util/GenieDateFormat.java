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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.text.FieldPosition;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Optional;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * The date format used by Genie.
 * Override the serialization to output a date string containing milliseconds and string representation of the timezone.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class GenieDateFormat extends StdDateFormat {

    /**
     * Pattern to validate (e.g., in tests) the expected date format this class produces.
     */
    @VisibleForTesting
    public static final Pattern VALID_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}Z$");
    private static final TimeZone TIMEZONE = TimeZone.getTimeZone("UTC");

    /**
     * Constructor.
     */
    public GenieDateFormat() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StringBuffer format(final Date date, final StringBuffer toAppendTo, final FieldPosition fieldPosition) {
        // StdDateFormat and ISO8601Utils do not offer a way to format consistent with the current Genie API format
        // (i.e. include milliseconds and use 'Z' as timezone). Therefore, format manually for backward compatibility.
        final Calendar calendar = new GregorianCalendar(TIMEZONE);
        calendar.setTime(date);
        return toAppendTo.append(
            String.format(
                "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE),
                calendar.get(Calendar.SECOND),
                calendar.get(Calendar.MILLISECOND)
            )
        );
    }

    /**
     * JsonSerializer to output (Optional) Date using GenieDateFormat.
     *
     * @since 3.3.11
     */
    public static class OptionalDateJsonSerializer extends JsonSerializer<Optional<Date>> {
        /**
         * {@inheritDoc}
         */
        @Override
        public void serialize(
            final Optional<Date> optionalDate,
            final JsonGenerator gen,
            final SerializerProvider serializers
        ) throws IOException {
            if (optionalDate.isPresent()) {
                gen.writeString(new GenieDateFormat().format(optionalDate.get()));
            } else {
                gen.writeNull();
            }
        }
    }
}
