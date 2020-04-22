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
package com.netflix.genie.common.internal.properties

import spock.lang.Specification

class RegexDirectoryManifestPropertiesSpec extends Specification {

    RegexDirectoryManifestProperties properties

    def setup() {
        this.properties = new RegexDirectoryManifestProperties()
    }

    def "Defaults, setters, getters"() {
        expect:
        properties.isCaseSensitiveMatching()
        properties.getFileRejectPatterns().isEmpty()
        properties.getDirectoryRejectPatterns().isEmpty()
        properties.getDirectoryTraversalRejectPatterns().isEmpty()

        when:
        properties.setCaseSensitiveMatching(false)
        properties.setFileRejectPatterns(["bbb", "BBB"] as Set)
        properties.setDirectoryRejectPatterns(["ddd", "DDD"] as Set)
        properties.setDirectoryTraversalRejectPatterns(["fff", "FFF"] as Set)

        then:
        !properties.isCaseSensitiveMatching()
        properties.getFileRejectPatterns() == ["bbb", "BBB"] as Set
        properties.getDirectoryRejectPatterns() == ["ddd", "DDD"] as Set
        properties.getDirectoryTraversalRejectPatterns() == ["fff", "FFF"] as Set
    }
}
