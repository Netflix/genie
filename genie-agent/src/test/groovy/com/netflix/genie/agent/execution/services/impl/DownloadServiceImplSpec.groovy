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

import com.netflix.genie.agent.execution.exceptions.DownloadException
import com.netflix.genie.agent.execution.services.DownloadService
import com.netflix.genie.agent.execution.services.FetchingCacheService
import com.netflix.genie.test.categories.UnitTest
import org.apache.commons.lang3.tuple.Pair
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import org.mockito.internal.util.collections.Sets
import spock.lang.Specification

import java.util.stream.Collectors

@Category(UnitTest)
class DownloadServiceImplSpec extends Specification {
    @Rule
    TemporaryFolder temporaryFolder

    FetchingCacheService cacheService
    DownloadServiceImpl downloadService
    DownloadService.Manifest manifest
    File cacheDir
    File jobDir
    File fileInCache
    String fileContents = "Example content of a file in cache\n"

    void setup() {
        temporaryFolder.create()
        cacheService = Mock()
        downloadService = new DownloadServiceImpl(cacheService)
        manifest = Mock()
        cacheDir = temporaryFolder.newFolder("cache")
        jobDir = temporaryFolder.newFolder("job")
        fileInCache = new File(cacheDir, "cached-file")
        fileInCache.createNewFile()
        fileInCache.write(fileContents)
    }

    void cleanup() {
    }

    def "Download empty manifest"() {
        when:
        downloadService.download(downloadService.newManifestBuilder().build())

        then:
        noExceptionThrown()
    }

    def "Download"() {
        setup:
        Map<URI, File> manifestMap = [
                (new URI("http:/foo/bar.txt")): new File(jobDir, "bar.txt"),
                (new URI("http:/foo/baz.txt")): new File(jobDir, "baz.txt"),
        ]

        Set<Pair<URI, File>> entries = manifestMap.entrySet()
        .stream().map({
            entry -> Pair.of(entry.getKey(), entry.getValue())
        }).collect(Collectors.toSet())

        when:
        downloadService.download(manifest)

        then:
        1 * manifest.getTargetDirectories() >> manifestMap.values().stream().map({File f -> f.getParentFile()}).collect(Collectors.toSet())
        1 * manifest.getTargetFiles() >> (Set<File>) manifestMap.values()
        1 * manifest.getEntries() >> entries
        1 * cacheService.get(entries)
    }

    def "Missing destination folder"() {
        setup:
        jobDir.delete()

        when:
        downloadService.download(manifest)

        then:
        1 * manifest.getTargetDirectories() >> Sets.newSet(jobDir)

        then:
        thrown(DownloadException)
    }

    def "Destination folder is a file"() {
        setup:
        jobDir.delete()
        jobDir.createNewFile()

        when:
        downloadService.download(manifest)

        then:
        1 * manifest.getTargetDirectories() >> Sets.newSet(jobDir)

        then:
        thrown(DownloadException)
    }

    def "Target file exists"() {
        setup:
        Map<URI, File> manifestMap = [
                (new URI("http:/foo/bar.txt")): new File(jobDir, "bar.txt"),
        ]
        manifestMap.values().iterator().next().createNewFile()

        when:
        downloadService.download(manifest)

        then:
        1 * manifest.getTargetDirectories() >> manifestMap.values().stream().map({File f -> f.getParentFile()}).collect(Collectors.toSet())
        1 * manifest.getTargetFiles() >> (Set<File>) manifestMap.values()
        thrown(DownloadException)
    }

    def "Cache throws DownloadException"() {
        setup:
        Set<Pair<URI, File>> entries = new HashSet<>()

        when:
        downloadService.download(manifest)

        then:
        1 * manifest.getTargetDirectories() >> new HashSet<File>()
        1 * manifest.getTargetFiles() >> new HashSet<File>()
        1 * manifest.getEntries() >> entries
        1 * cacheService.get(entries) >> {throw new DownloadException("test")}
        thrown(DownloadException)
    }

    def "Cache throws IOException"() {
        setup:
        Set<Pair<URI, File>> entries = new HashSet<>()

        when:
        downloadService.download(manifest)

        then:
        1 * manifest.getTargetDirectories() >> new HashSet<File>()
        1 * manifest.getTargetFiles() >> new HashSet<File>()
        1 * manifest.getEntries() >> entries
        1 * cacheService.get(entries) >> {throw new IOException("test")}
        thrown(DownloadException)
    }
}
