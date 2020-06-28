/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.data.services.impl.jpa.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;

import javax.annotation.Nullable;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/**
 * An {@link AttributeConverter} to convert {@link JsonNode} objects into their String representations for storage
 * and vice versa.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Converter
public class JsonAttributeConverter implements AttributeConverter<JsonNode, String> {

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public String convertToDatabaseColumn(@Nullable final JsonNode attribute) {
        if (attribute == null) {
            return null;
        }

        try {
            return GenieObjectMapper.getMapper().writeValueAsString(attribute);
        } catch (final JsonProcessingException e) {
            throw new GenieRuntimeException("Unable to convert JsonNode to a JSON string for storing in database", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public JsonNode convertToEntityAttribute(@Nullable final String dbData) {
        if (dbData == null) {
            return null;
        }

        try {
            return GenieObjectMapper.getMapper().readTree(dbData);
        } catch (final JsonProcessingException e) {
            throw new GenieRuntimeException("Unable to convert: (" + dbData + ") to JsonNode", e);
        }
    }
}
