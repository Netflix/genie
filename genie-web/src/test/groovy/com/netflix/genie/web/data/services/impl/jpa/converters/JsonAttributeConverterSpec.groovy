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
package com.netflix.genie.web.data.services.impl.jpa.converters

import com.fasterxml.jackson.databind.JsonNode
import com.netflix.genie.common.external.util.GenieObjectMapper
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import spock.lang.Specification

/**
 * Specifications for {@link JsonAttributeConverter}.
 *
 * @author tgianos
 */
class JsonAttributeConverterSpec extends Specification {

    JsonAttributeConverter converter

    def setup() {
        this.converter = new JsonAttributeConverter()
    }

    def "can make round trip"() {
        JsonNode sourceNode = GenieObjectMapper.getMapper().createObjectNode()
        sourceNode.put(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        sourceNode.put(UUID.randomUUID().toString(), 1)
        sourceNode.put(UUID.randomUUID().toString(), false)

        when:
        String sourceString = this.converter.convertToDatabaseColumn(sourceNode)
        JsonNode resultNode = this.converter.convertToEntityAttribute(sourceString)
        String resultString = this.converter.convertToDatabaseColumn(resultNode)

        then:
        sourceNode == resultNode
        sourceString == resultString
    }

    def "null inputs return null"() {
        expect:
        this.converter.convertToDatabaseColumn(null) == null
        this.converter.convertToEntityAttribute(null) == null
    }

    def "bad json string throws expected exception"() {
        def notJson = UUID.randomUUID().toString()

        when:
        this.converter.convertToEntityAttribute(notJson)

        then:
        thrown(GenieRuntimeException)
    }
}
