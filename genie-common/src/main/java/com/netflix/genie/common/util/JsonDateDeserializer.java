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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * Used to de-serialize dates from strings in Json payloads.
 *
 * @author tgianos
 * @since 2.0.0
 */
public class JsonDateDeserializer extends JsonDeserializer<Date> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Date deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
        final DateFormat format = new ISO8601DateFormat();

        final String text = parser.getText();

        if (StringUtils.isBlank(text)) {
            return null;
        }

        try {
            return format.parse(text);
        } catch (final ParseException pe) {
            throw new IOException(pe);
        }
    }
}
