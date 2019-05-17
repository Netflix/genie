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
package com.netflix.genie.common.internal.configs

import com.netflix.genie.common.internal.dto.DirectoryManifest
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes

class CommonServicesAutoConfigurationSpec extends Specification {

    DirectoryManifest.Filter filter = new CommonServicesAutoConfiguration().directoryManifestFilter()
    BasicFileAttributes attributes = Mock(BasicFileAttributes)

    @Unroll
    def "DirectoryManifestFilter for #pathString"() {
        setup:
        Path path = Paths.get(pathString)

        when:
        boolean fileIncluded = filter.includeFile(path, attributes)
        boolean dirIncluded = filter.includeDirectory(path, attributes)
        boolean treeIncluded = filter.walkDirectory(path, attributes)

        then:
        fileIncluded == expectFileIncluded
        dirIncluded == expectDirIncluded
        treeIncluded == expectTreeIncluded

        where:
        expectFileIncluded | expectDirIncluded | expectTreeIncluded | pathString
        true               | true              | false              | "/foo/genie/application/hadoop/dependencies"
        true               | true              | false              | "/foo/genie/application/hadoop/dependencies/"
        true               | true              | true               | "/foo/hadoop/dependencies"
        true               | true              | true               | "/foo/genie/application/hadoop/xdependencies"
        true               | true              | true               | "/foo/dependencies"
        true               | true              | true               | "/dependencies"
    }
}
