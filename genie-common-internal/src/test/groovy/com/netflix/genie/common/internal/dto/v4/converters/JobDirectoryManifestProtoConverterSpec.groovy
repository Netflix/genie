/*
 *
 *  Copyright 2019 Netflix, Inc.
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

package com.netflix.genie.common.internal.dto.v4.converters

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.genie.common.internal.dto.JobDirectoryManifest
import com.netflix.genie.common.internal.exceptions.GenieConversionException
import com.netflix.genie.proto.JobFileManifestMessage
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest)
class JobDirectoryManifestProtoConverterSpec extends Specification {
    ObjectMapper objectMapper
    JobDirectoryManifestProtoConverter converter
    static final String JSON_MANIFEST = "{ fake json serialization of manifest }"

    void setup() {
        this.objectMapper = Mock(ObjectMapper)
        this.converter = new JobDirectoryManifestProtoConverter(this.objectMapper)
    }

    def "Manifest to message to manifest"() {
        setup:
        String jobId = "123456"
        JobDirectoryManifest manifest = Mock(JobDirectoryManifest)

        when:
        JobFileManifestMessage message = this.converter.manifestToProtoMessage(jobId, manifest)

        then:
        1 * objectMapper.writeValueAsString(manifest) >> JSON_MANIFEST
        message.getJobId() == jobId
        message.getManifestJsonString() == JSON_MANIFEST

        when:
        JobDirectoryManifest loadedManifest = converter.toManifest(message)

        then:
        1 * objectMapper.readValue(JSON_MANIFEST, JobDirectoryManifest.class) >> manifest
        loadedManifest == manifest
    }

    def "Manifest JSON serialization error"() {
        setup:
        JobDirectoryManifest manifest = Mock(JobDirectoryManifest)
        Exception exception = new JsonProcessingException("...")

        when:
        this.converter.manifestToProtoMessage("...", manifest)

        then:
        1 * objectMapper.writeValueAsString(manifest) >> {throw exception}
        Exception e = thrown(GenieConversionException)
        e.getCause() == exception
    }

    def "Manifest JSON parsing error"() {
        setup:
        Exception exception = new IOException("...")

        when:
        this.converter.toManifest(JobFileManifestMessage.getDefaultInstance())

        then:
        1 * objectMapper.readValue(_, JobDirectoryManifest.class) >> {throw exception}
        Exception e = thrown(GenieConversionException)
        e.getCause() == exception
    }
}
