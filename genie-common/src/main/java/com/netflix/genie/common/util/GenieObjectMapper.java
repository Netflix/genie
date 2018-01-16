/*
 *
 *  Copyright 2018 Netflix, Inc.
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.ZoneId;
import java.util.TimeZone;

/**
 * A singleton for sharing a Jackson Object Mapper instance across Genie and not having to redefine the Object Mapper
 * everywhere.
 *
 * @author tgianos
 * @since 4.0.0
 */
public final class GenieObjectMapper {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));

    private GenieObjectMapper() {
    }

    /**
     * Get the preconfigured Object Mapper used across Genie for consistency.
     *
     * @return The object mapper to use
     */
    public static ObjectMapper getMapper() {
        return MAPPER;
    }
}
