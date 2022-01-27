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
package com.netflix.genie.common.util

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant

/**
 * Specifications for {@link JsonUtils}.
 *
 * @author tgianos
 */
class JsonUtilsSpec extends Specification {

    @Unroll
    def "Can split #commandArgsString into expected #expectedArgsList"() {
        expect:
        JsonUtils.splitArguments(commandArgsString) == expectedArgsList

        where:
        commandArgsString                       | expectedArgsList
        "/bin/bash -xc 'echo hello world!'"     | ["/bin/bash", "-xc", "echo hello world!"]
        "/bin/bash -xc 'echo \"hello\" world!'" | ["/bin/bash", "-xc", "echo \"hello\" world!"]
        "arg1   arg2\targ3\narg4\r'arg5' '''"   | ["arg1", "arg2", "arg3", "arg4", "arg5", "'"]
        "foo/bar --hello '\$world'"             | ["foo/bar", "--hello", "\$world"]
        "foo/bar \" --hello\" \"\$world\""      | ["foo/bar", " --hello", "\$world"]
        "foo/bar \"'hello'\""                   | ["foo/bar", "'hello'"]
        "\\\"foo bar\\\""                       | ["\\\"foo", "bar\\\""]
        "'some.property=foo bar'"               | ["some.property=foo bar"]
        "'some.property=\"foo bar\"'"           | ["some.property=\"foo bar\""]
        "some.property='foo bar'"               | ["some.property='foo", "bar'"]
        "some.property=\\\"foo bar\\\""         | ["some.property=\\\"foo", "bar\\\""]
    }

    @Unroll
    def "Can join #commandArgsList into expected #expectedString"() {
        expect:
        JsonUtils.joinArguments(commandArgsList) == expectedString

        where:
        commandArgsList                               | expectedString
        ["/bin/bash", "-xc", "echo hello world!"]     | "'/bin/bash' '-xc' 'echo hello world!'"
        ["/bin/bash", "-xc", "echo \"hello\" world!"] | "'/bin/bash' '-xc' 'echo \"hello\" world!'"
        ["arg1", "arg2", "arg3", "arg4", "arg5", "'"] | "'arg1' 'arg2' 'arg3' 'arg4' 'arg5' '''"
        ["foo/bar", "--hello", "\$world"]             | "'foo/bar' '--hello' '\$world'"
    }

    @Unroll
    def "Can serialize instant #instant to millisecond precision string #instantString"() {
        def generator = Mock(JsonGenerator)
        def serializer = new JsonUtils.InstantMillisecondSerializer()

        when:
        serializer.serialize(instant, generator, Mock(SerializerProvider))

        then:
        1 * generator.writeString(instantString)

        where:
        instant                                  | instantString
        Instant.ofEpochMilli(52)                 | "1970-01-01T00:00:00.052Z"
        Instant.ofEpochMilli(52000)              | "1970-01-01T00:00:52Z"
        Instant.ofEpochMilli(52000).plusNanos(7) | "1970-01-01T00:00:52Z"
        Instant.ofEpochMilli(52001)              | "1970-01-01T00:00:52.001Z"
        Instant.ofEpochMilli(52001).plusNanos(1) | "1970-01-01T00:00:52.001Z"
    }

    @Unroll
    def "Can serialize optional instant #instant to millisecond precision string #instantString"() {
        def generator = Mock(JsonGenerator)
        def serializer = new JsonUtils.OptionalInstantMillisecondSerializer()

        when:
        serializer.serialize((Optional<Instant>) instant, generator, Mock(SerializerProvider))

        then:
        if (instant.isPresent()) {
            1 * generator.writeString(instantString)
        }
        else {
            1 * generator.writeNull()
        }

        where:
        instant                                               | instantString
        Optional.of(Instant.ofEpochMilli(52))                 | "1970-01-01T00:00:00.052Z"
        Optional.of(Instant.ofEpochMilli(52000))              | "1970-01-01T00:00:52Z"
        Optional.of(Instant.ofEpochMilli(52000).plusNanos(7)) | "1970-01-01T00:00:52Z"
        Optional.of(Instant.ofEpochMilli(52001))              | "1970-01-01T00:00:52.001Z"
        Optional.of(Instant.ofEpochMilli(52001).plusNanos(1)) | "1970-01-01T00:00:52.001Z"
        Optional.empty()                                      | "blah"
    }
}
