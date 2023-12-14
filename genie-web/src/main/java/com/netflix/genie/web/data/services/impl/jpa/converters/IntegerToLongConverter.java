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

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import jakarta.annotation.Nullable;

/**
 * An {@link AttributeConverter} to convert {@link Integer} objects into {@link Long} for storage and vice versa.
 *
 * @author ltian
 * @since 4.3.0
 */
@Converter
public class IntegerToLongConverter implements AttributeConverter<Long, Integer> {

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public Integer convertToDatabaseColumn(@Nullable final Long attribute) {
        if (attribute == null) {
            return null;
        }

        return attribute.intValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public Long convertToEntityAttribute(@Nullable final Integer dbData) {
        if (dbData == null) {
            return null;
        }

        return Long.valueOf(dbData);
    }
}
