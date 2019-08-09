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

import spock.lang.Specification
import spock.lang.Unroll

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
}
