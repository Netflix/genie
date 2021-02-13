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
package com.netflix.genie.common.internal.util

import com.netflix.genie.common.internal.properties.RegexDirectoryManifestProperties
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes

class RegexDirectoryManifestFilterSpec extends Specification {

    static String basePath = "/tmp/genie/jobs/" + UUID.randomUUID().toString() + "/"
    static Path logsDirPath = Paths.get(basePath + "genie/logs")
    static Path agentLogPath = Paths.get(basePath + "genie/logs/agent.log")

    RegexDirectoryManifestProperties filterProperties
    BasicFileAttributes attributes

    void setup() {
        attributes = Mock(BasicFileAttributes)
        this.filterProperties = new RegexDirectoryManifestProperties()
    }

    @Unroll
    def "Filter file with patterns: #patterns"() {
        setup:
        filterProperties.setFileRejectPatterns(patterns as Set)
        RegexDirectoryManifestFilter filter = new RegexDirectoryManifestFilter(filterProperties)

        when:
        boolean includedInManifest = filter.includeFile(agentLogPath, attributes)

        then:
        includedInManifest == expectIncludedInManifest

        where:
        expectIncludedInManifest | patterns
        true                     | []
        true                     | ["foo"]
        false                    | ["foo", ".*\\.log"]
        false                    | ["foo", ".*/logs/.*\\.log"]
    }

    @Unroll
    def "Filter directory with patterns: #patterns"() {
        setup:
        filterProperties.setDirectoryRejectPatterns(patterns as Set)
        RegexDirectoryManifestFilter filter = new RegexDirectoryManifestFilter(filterProperties)

        when:
        boolean includedInManifest = filter.includeDirectory(logsDirPath, attributes)

        then:
        includedInManifest == expectIncludedInManifest

        where:
        expectIncludedInManifest | patterns
        true                     | []
        true                     | ["foo"]
        false                    | ["foo", ".*/logs"]
    }

    @Unroll
    def "Skip directory patterns: #patterns"() {
        setup:
        filterProperties.setDirectoryTraversalRejectPatterns(patterns as Set)
        RegexDirectoryManifestFilter filter = new RegexDirectoryManifestFilter(filterProperties)

        when:
        boolean includedInManifest = filter.walkDirectory(logsDirPath, attributes)

        then:
        includedInManifest == expectIncludedInManifest

        where:
        expectIncludedInManifest | patterns
        true                     | []
        true                     | ["foo"]
        false                    | ["foo", ".*/logs"]
    }

    @Unroll
    def "Case insensitive: #caseSensitive"() {
        setup:
        filterProperties.setCaseSensitiveMatching(caseSensitive)
        filterProperties.setFileRejectPatterns([".*/Agent\\.log"] as Set)
        filterProperties.setDirectoryRejectPatterns([".*/LOGS"] as Set)
        RegexDirectoryManifestFilter filter = new RegexDirectoryManifestFilter(filterProperties)

        when:
        boolean fileIncluded = filter.includeFile(agentLogPath, attributes)
        boolean directoryIncluded = filter.includeDirectory(logsDirPath, attributes)

        then:
        directoryIncluded == expectIncludedInManifest
        fileIncluded == expectIncludedInManifest

        where:
        caseSensitive | expectIncludedInManifest
        true          | true
        false         | false
    }
}
