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

package com.netflix.genie.agent.utils

import org.springframework.core.io.ClassPathResource
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

class EnvUtilsSpec extends Specification {

    static Map<String, String> emptyMap = [:]
    static Map<String, String> fooBarMap = [
            "foo": "bar",
            "FOO": "BAR",
            "F-O-O": "B A R"
    ]
    static Map<String, String> fooBarMultiLineMap = [
            "foo": "bar",
            "FOO": "BAR",
            "multi-line": "1\n2\n3"
    ]
    static String testFile = EnvUtilsSpec.class.getSimpleName() + "-test1.txt"

    @Unroll
    def "parseEnvStream '#streamString'"(
            String streamString,
            Map<String, String> expectedValues
    ) {
        setup:
        InputStream stream = new ByteArrayInputStream(streamString.getBytes(StandardCharsets.UTF_8))

        when:
        Map<String, String> parsedEnv = EnvUtils.parseEnvStream(stream)

        then:
        expectedValues.size() == parsedEnv.size()
        expectedValues == parsedEnv

        where:
        streamString                                         | expectedValues
        ""                                                   | emptyMap
        "\n"                                                 | emptyMap
        "\n \n"                                              | emptyMap
        "foo='bar'\nFOO='BAR'\nF-O-O='B A R'\n"              | fooBarMap
        "\nF-O-O='B A R'\nfoo='bar'\nFOO='BAR'"              | fooBarMap
        "\nfoo='bar'\nFOO='BAR'\nF-O-O='B A R'\n"            | fooBarMap
        "\n\nfoo='bar'\n\nFOO='BAR'\n\nF-O-O='B A R'\n\n"    | fooBarMap
        "\nF-O-O='B A R'\nfoo='bar'\nFOO='BAR'\n"            | fooBarMap
        "foo='bar'\nmulti-line='1\n2\n3'\n\nFOO='BAR'"       | fooBarMultiLineMap
        "foo='bar'\nFOO='BAR'\nmulti-line='1\n2\n3'"         | fooBarMultiLineMap
    }

    @Unroll
    def "parseEnvStream error '#streamString'"(
            String streamString
    ) {
        setup:
        InputStream stream = new ByteArrayInputStream(streamString.getBytes(StandardCharsets.UTF_8))

        when:
        EnvUtils.parseEnvStream(stream)

        then:
        thrown(EnvUtils.ParseException)

        where:
        streamString       | _
        "foo"              | _
        "foo\nfoo"         | _
        "=foo"             | _
        "foo='bar\nbar\n"  | _
        "foo='bar\nbar'x"  | _
    }

    def "Parse nonexistent file"() {
        when:
        EnvUtils.parseEnvFile(new File("/this/file/does/not/exist"))

        then:
        thrown(IOException)
    }

    def "Parse file"() {
        setup:
        Path tempLocalFile = Files.createTempFile(null, null)
        Files.copy(
                new ClassPathResource(testFile).getInputStream(),
                tempLocalFile,
                StandardCopyOption.REPLACE_EXISTING
        )

        when:
        Map<String, String> envMap = EnvUtils.parseEnvFile(tempLocalFile.toFile())

        then:
        42 == envMap.size()
        "w0t1p0:E3EAD46A-721E-4B1C-A56F-4D9C2EE0595E" == envMap.get("ITERM_SESSION_ID")
        "" == envMap.get("n")
        "/usr/local/bin/mate -w" == envMap.get("EDITOR")
        "foo\nbar" == envMap.get("multiline")
    }

    @Unroll
    def "Shell variable substitution: #inputString"(
            String inputString,
            String expectedString,
            Map<String, String> envMap
    ) {
        when:
        String outputString = EnvUtils.expandShellVariables(inputString, envMap)

        then:
        expectedString == outputString

        where:
        inputString             | expectedString      | envMap
        "foo"                   | "foo"               | [:]
        "\${123}"               | "\${123}"           | [:]
        "\${foo}"               | "bar"               | [foo: "bar"]
        "\${_foo}"              | "bar"               | [_foo: "bar"]
        "\${foo_foo}"           | "bar"               | [foo_foo: "bar"]
        "\${foo_2}"             | "bar"               | [foo_2: "bar"]
        "foo \$foo \${foo} foo" | "foo bar bar foo"   | [foo: "bar"]
        "foo \$FOO \${FOO} foo" | "foo bar bar foo"   | [FOO: "bar", foo: "x"]
        "\$foo\$foo\$foo"       | "barbarbar"         | [foo: "bar"]
        "\$FOO\$FOO\$FOO"       | "barbarbar"         | [FOO: "bar"]
        "\"\$/\$FOO/\$BAR/\$\"" | "\"\$/foo/bar/\$\"" | [FOO: "foo", BAR: "bar"]
    }

}
