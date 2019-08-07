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
package com.netflix.genie.web.properties.converters

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link URIPropertyConverter}.
 *
 * @author tgianos
 */
class URIPropertyConverterSpec extends Specification {

    @Unroll
    def "Can convert #uriString to URI with expected scheme #expectedScheme"() {
        def converter = new URIPropertyConverter()

        when:
        def uri = converter.convert(uriString)

        then:
        uri != null
        uri.getScheme() == expectedScheme
        noExceptionThrown()

        where:
        uriString                              | expectedScheme
        "file:///tmp/genie/archives"           | "file"
        "file:/tmp/genie/archives"             | "file"
        "s3://mybucket/somewhere?blah=myValue" | "s3"
        "agent://what-the-what?jobId=1133"     | "agent"
    }

    @Unroll
    def "Bad URI #badUri throws #exception"() {
        def converter = new URIPropertyConverter()

        when:
        converter.convert(badUri)

        then:
        thrown(exception)

        where:
        badUri          | exception
        "I'm not a uri" | IllegalArgumentException
        null            | NullPointerException
        "/tmp/genie/"   | IllegalArgumentException
    }
}
