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

package com.netflix.genie.common.internal.util

import org.assertj.core.util.Sets
import spock.lang.Specification
import spock.lang.Unroll

import java.util.regex.Pattern
import java.util.stream.Collectors

class RegexRuleSetSpec extends Specification {

    @Unroll
    def "Build and match: #inputString"(String inputString, boolean expectAccepted) {
        setup:
        RegexRuleSet ruleset = new RegexRuleSet.Builder(RegexRuleSet.Response.REJECT)
                .addRule(Pattern.compile(".*foo.*", Pattern.CASE_INSENSITIVE), RegexRuleSet.Response.ACCEPT)
                .addRule("^ThisParticularString\$", RegexRuleSet.Response.ACCEPT)
                .addRule(".*1234.*", RegexRuleSet.Response.REJECT)
                .addRule(".*[0-9]+.*", RegexRuleSet.Response.ACCEPT)
                .build()

        when:
        boolean accepted = ruleset.accept(inputString)

        then:
        expectAccepted == accepted

        where:
        inputString                  | expectAccepted
        "aFoo"                       | true
        "aaFoOaa"                    | true
        "ThisParticularString"       | true
        "notThisParticularString"    | false
        "1234"                       | false
        "012345"                     | false
        "654321"                     | true
        "Bar"                        | false
    }

    def "Users whitelist"() {
        setup:
        String[] whitelistedUsers = ["root", "admin"]
        String[] allUsers = ["jdoe", "root", "bfoo", "fbar", "administrator", "superuser", "admin"]
        def whitelist = RegexRuleSet.buildWhitelist(whitelistedUsers)

        when:
        def acceptedUsers = allUsers.toList().stream().filter({u -> whitelist.accept(u)}).collect(Collectors.toSet())

        then:
        Sets.newHashSet(whitelistedUsers.toList()) == acceptedUsers
    }

    def "IPs blacklist"() {
        setup:
        String[] blacklistedSubnets = [
                Pattern.compile("^10\\..*"),
                Pattern.compile("192\\.168\\..*"),
                Pattern.compile("127\\..*"),
        ]
        def blacklist = RegexRuleSet.buildBlacklist(blacklistedSubnets)

        expect:
        blacklist.accept("123.4.5.6")
        blacklist.accept("192.192.5.6")
        blacklist.accept("192.192.5.6")
        blacklist.accept("whatever, really")
        blacklist.reject("127.0.0.1")
        blacklist.reject("10.0.0.1")
        blacklist.reject("192.168.foo.bar")
    }

}
