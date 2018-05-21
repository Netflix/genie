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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;

/**
 * Used to serialize dates into json in an expected format.
 *
 * @author tgianos
 * @since 2.0.0
 */
public class JsonDateSerializer extends JsonSerializer<Date> {

    private final DateFormat dateFormat = new GenieDateFormat(); //ISO8601, UTC timezone

    /**
     * {@inheritDoc}
     */
    @Override
    public void serialize(final Date date, final JsonGenerator gen, final SerializerProvider provider)
            throws IOException {
        if (date == null) {
            gen.writeString((String) null);
        } else {
            gen.writeString(dateFormat.format(date));
        }
    }
}

