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
package com.netflix.genie.common.external.util;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.PropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;

/**
 * A singleton for sharing a Jackson Object Mapper instance across Genie and not having to redefine the Object Mapper
 * everywhere.
 *
 * @author tgianos
 * @since 4.0.0
 */
public final class GenieObjectMapper {

    /**
     * The name of the {@link PropertyFilter} used to serialize Genie exceptions.
     */
    public static final String EXCEPTIONS_FILTER_NAME = "GenieExceptionsJSONFilter";

    /**
     * The actual filter used for Genie exceptions. Ignores all fields except the ones listed below.
     */
    private static final PropertyFilter EXCEPTIONS_FILTER = new SimpleBeanPropertyFilter.FilterExceptFilter(
        ImmutableSet.of(
            "errorCode",
            "message"
        )
    );

    /**
     * Classes annotated with {@link JsonFilter} request to be processed with a certain filter by name.
     * This map encodes the name to filter map in a form suitable for the {@link SimpleFilterProvider} below.
     */
    private static final Map<String, PropertyFilter> FILTERS_MAP = ImmutableMap.of(
        EXCEPTIONS_FILTER_NAME, EXCEPTIONS_FILTER
    );

    /**
     * The {@link FilterProvider} to be installed in the {@link ObjectMapper}.
     */
    public static final FilterProvider FILTER_PROVIDER = new SimpleFilterProvider(FILTERS_MAP);

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
        .setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")))
        .setFilterProvider(FILTER_PROVIDER);

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
