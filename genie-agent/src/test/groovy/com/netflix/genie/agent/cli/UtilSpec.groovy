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
package com.netflix.genie.agent.cli

import spock.lang.Specification
import spock.lang.Unroll

class UtilSpec extends Specification {

    @Unroll
    def "Mangle and unmangle arguments #args"(String[] args) {
        setup:
        String[] mangledArgs = Util.mangleBareDoubleDash(args)
        String[] unmangledArgs = Util.unmangleBareDoubleDash(mangledArgs)
        expect:
        args == unmangledArgs

        where:
        _ | args
        _ | [] as String[]
        _ | ["--foo", "f"] as String[]
        _ | ["---", "-"] as String[]
        _ | ["--foo", "f", "--", "--bar=b"] as String[]
    }

    @Unroll
    def "Split options and operands: #args"() {
        setup:

        expect:
        expectedOptions == Util.getOptionArguments(args)
        expectedOperands == Util.getOperandArguments(args)

        where:
        args                                               | expectedOptions                | expectedOperands
        [] as String[]                                     | [] as String[]                 | [] as String[]
        ["--"] as String[]                                 | [] as String[]                 | [] as String[]
        ["--foo", "f"] as String[]                         | ["--foo", "f"] as String[]     | [] as String[]
        ["--foo", "f", "--"] as String[]                   | ["--foo", "f"] as String[]     | [] as String[]
        ["--", "--foo", "f"] as String[]                   | [] as String[]                 | ["--foo", "f"] as String[]
        ["--", "--foo", "f"] as String[]                   | [] as String[]                 | ["--foo", "f"] as String[]
        ["--bar", "--baz", "--", "--foo", "f"] as String[] | ["--bar", "--baz"] as String[] | ["--foo", "f"] as String[]
    }
}
