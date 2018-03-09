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

package com.netflix.genie.agent.execution.services.impl

import com.netflix.genie.agent.execution.services.DownloadService
import spock.lang.Specification

class DownloadServiceManifestImplSpec extends Specification {
    URI uri1 = new URI("http://foo.com/file1.txt")
    URI uri2 = new URI("http://foo.com/file2.txt")
    File file1 = new File("/foo/file1.txt")
    File file2 = new File("/foo/file2.txt")

    void setup() {
    }

    void cleanup() {
    }

    def "Build"() {
        when:
        DownloadService.Manifest manifest = new DownloadServiceImpl().newManifestBuilder()
                .addFileWithTargetFile(uri1, file1)
                .addFileWithTargetDirectory(uri2, file2.getParentFile())
                .build()

        then:
        manifest != null
        manifest.getTargetDirectories().size() == 1
        manifest.getTargetDirectories().contains(file2.getParentFile())
        manifest.getTargetFiles().size() == 2
        manifest.getTargetFiles().contains(file1)
        manifest.getTargetFiles().contains(file2)
        manifest.getSourceFileUris().size() == 2
        manifest.getSourceFileUris().contains(uri1)
        manifest.getSourceFileUris().contains(uri2)
        manifest.getEntries().size() == 2
    }

    def "Build with duplicate source"() {
        when:
        DownloadService.Manifest manifest = new DownloadServiceImpl().newManifestBuilder()
                .addFileWithTargetFile(uri1, file1)
                .addFileWithTargetFile(uri1, file2)
                .build()

        then:
        manifest != null
        manifest.getTargetDirectories().size() == 1
        manifest.getTargetDirectories().contains(file2.getParentFile())
        manifest.getTargetFiles().size() == 1
        manifest.getTargetFiles().contains(file2)
        manifest.getSourceFileUris().size() == 1
        manifest.getSourceFileUris().contains(uri1)
        manifest.getEntries().size() == 1
    }

    def "Build empty"() {
        when:
        DownloadService.Manifest manifest = new DownloadServiceImpl().newManifestBuilder().build()

        then:
        manifest != null
        manifest.getTargetDirectories().isEmpty()
        manifest.getTargetFiles().isEmpty()
        manifest.getSourceFileUris().isEmpty()
        manifest.getEntries().isEmpty()
    }

    def "Build with duplicate target"() {
        when:
        new DownloadServiceImpl().newManifestBuilder()
            .addFileWithTargetFile(uri1, file1)
            .addFileWithTargetFile(uri2, file1)
            .build()

        then:
        thrown(IllegalArgumentException)
    }

    def "Build with invalid path-less uri"() {
        when:
        new DownloadServiceImpl().newManifestBuilder()
                .addFileWithTargetDirectory(new URI("http://foo.com"), file1.getParentFile())

        then:
        thrown(IllegalArgumentException)
    }

    def "Build with invalid name-less uri"() {
        when:
        new DownloadServiceImpl().newManifestBuilder()
                .addFileWithTargetDirectory(new URI("http://foo.com/"), file1.getParentFile())

        then:
        thrown(IllegalArgumentException)
    }

    def "Lookup target"() {
        setup:
        DownloadService.Manifest manifest = new DownloadServiceImpl().newManifestBuilder()
                .addFileWithTargetFile(uri1, file1)
                .addFileWithTargetFile(uri2, file2)
                .build()
        when:
        def f1 = manifest.getTargetLocation(uri1)
        def f2 = manifest.getTargetLocation(uri2)

        then:
        f1 == file1
        f2 == file2
    }


    def "targetDirectories is immutable"() {
        setup:
        DownloadService.Manifest manifest = new DownloadServiceImpl().newManifestBuilder().build()

        when:
        manifest.getTargetDirectories().add(new File("/"))

        then:
        thrown(UnsupportedOperationException)
    }

    def "targetFiles is immutable"() {
        setup:
        DownloadService.Manifest manifest = new DownloadServiceImpl().newManifestBuilder().build()

        when:
        manifest.getTargetFiles().add(new File("/"))

        then:
        thrown(UnsupportedOperationException)
    }

    def "sourceFileUris is immutable"() {
        setup:
        DownloadService.Manifest manifest = new DownloadServiceImpl().newManifestBuilder().build()

        when:
        manifest.getSourceFileUris().add(new URI("http://www.foo.com/file.txt"))

        then:
        thrown(UnsupportedOperationException)
    }

    def "entries is immutable"() {
        setup:
        DownloadService.Manifest manifest = new DownloadServiceImpl().newManifestBuilder().build()

        when:
        manifest.getEntries().add(null)

        then:
        thrown(UnsupportedOperationException)
    }
}
