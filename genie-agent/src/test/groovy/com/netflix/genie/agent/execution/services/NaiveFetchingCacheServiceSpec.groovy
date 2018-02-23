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

package com.netflix.genie.agent.execution.services

import com.netflix.genie.agent.cli.ArgumentDelegates
import com.netflix.genie.agent.execution.exceptions.DownloadException
import com.netflix.genie.test.categories.UnitTest
import org.apache.commons.lang3.tuple.Pair
import org.assertj.core.util.Sets
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.Files

@Category(UnitTest.class)
class NaiveFetchingCacheServiceSpec extends Specification {
    ResourceLoader resourceLoader
    ArgumentDelegates.CacheArguments cacheArguments
    NaiveFetchingCacheService cache
    Resource resource
    URI uri

    @Rule
    TemporaryFolder temporaryFolder

    void setup() {
        temporaryFolder.create()
        resourceLoader = Mock()
        cacheArguments = Mock()
        cacheArguments.getCacheDirectory() >> temporaryFolder.getRoot()
        cache = new NaiveFetchingCacheService(resourceLoader, cacheArguments)
        resource = Mock()

        uri = new URI("https://my-server.com/path/to/config/config.xml")
    }

    void cleanup() {
    }

    def "Get resource not cached"() {
        setup:
        String fileContents = "example file contents\n"
        File targetFile = new File(temporaryFolder.getRoot(), "target")
        String expectedFilenameInCache = cache.getResourceCacheId(uri)
        File cachedFile = new File(temporaryFolder.getRoot(), expectedFilenameInCache)

        when:
        cache.get(uri, targetFile)

        then:
        1 * resourceLoader.getResource(_ as String) >> resource
        1 * resource.exists() >> true
        1 * resource.getInputStream() >> new ByteArrayInputStream(fileContents.getBytes())

        expect:
        cachedFile.exists()
        cachedFile.getText(StandardCharsets.UTF_8.toString()) == fileContents
        targetFile.exists()
        targetFile.getText(StandardCharsets.UTF_8.toString()) == fileContents
    }

    def "Get resource cached"() {
        setup:
        String expectedFilenameInCache = cache.getResourceCacheId(uri)
        File cachedFile = temporaryFolder.newFile(expectedFilenameInCache)
        String fileContents = "example file contents\n"
        cachedFile.write(fileContents, StandardCharsets.UTF_8.toString())
        File targetFile = new File(temporaryFolder.getRoot(), "target")

        when:
        cache.get(uri, targetFile)

        then:
        0 * resourceLoader.getResource(_ as String)

        expect:
        targetFile.exists()
        targetFile.getText(StandardCharsets.UTF_8.toString()) == fileContents
    }

    def "Get resource nonexistent"() {
        setup:
        File targetFile = new File(temporaryFolder.getRoot(), "target")

        when:
        cache.get(uri, targetFile)

        then:
        1 * resourceLoader.getResource(_ as String) >> resource
        1 * resource.exists() >> false
        thrown(DownloadException)
    }

    def "Download resource error"() {
        setup:
        File targetFile = new File(temporaryFolder.getRoot(), "target")
        temporaryFolder.delete()

        when:
        cache.get(uri, targetFile)

        then:
        1 * resourceLoader.getResource(_ as String) >> resource
        1 * resource.exists() >> true
        1 * resource.getInputStream() >> new ByteArrayInputStream("...".getBytes())
        thrown(IOException)
    }

    def "GetAll"() {
        setup:
        URI[] uris = [
                new URI("https://my-server.com/path/to/config/config.xml"),
                new URI("https://my-server.com/path/to/setup/setup.sh"),
                new URI("https://my-server.com/path/to/dependencies/bin.tar.gz")
        ]
        File[] targetFiles = [
                temporaryFolder.newFile("config.xml"),
                temporaryFolder.newFile("setup.sh"),
                temporaryFolder.newFile("bin.tar.gz")
        ]
        Resource[] resources = [
                Mock(Resource),
                Mock(Resource),
                Mock(Resource)
        ]

        when:
        cache.get(Sets.newHashSet([
                Pair.of(uris[0], targetFiles[0]),
                Pair.of(uris[1], targetFiles[1]),
                Pair.of(uris[2], targetFiles[2]),
        ]))

        then:
        1 * resourceLoader.getResource(uris[0].toString()) >> resources[0]
        1 * resourceLoader.getResource(uris[1].toString()) >> resources[1]
        1 * resourceLoader.getResource(uris[2].toString()) >> resources[2]
        1 * resources[0].exists() >> true
        1 * resources[1].exists() >> true
        1 * resources[2].exists() >> true
        1 * resources[0].getInputStream() >> new ByteArrayInputStream(uris[0].toString().getBytes(StandardCharsets.UTF_8))
        1 * resources[1].getInputStream() >> new ByteArrayInputStream(uris[1].toString().getBytes(StandardCharsets.UTF_8))
        1 * resources[2].getInputStream() >> new ByteArrayInputStream(uris[2].toString().getBytes(StandardCharsets.UTF_8))

        expect:
        targetFiles[0].getText(StandardCharsets.UTF_8.toString()) == uris[0].toString()
        targetFiles[1].getText(StandardCharsets.UTF_8.toString()) == uris[1].toString()
        targetFiles[2].getText(StandardCharsets.UTF_8.toString()) == uris[2].toString()

    }

    def "Construct: fail to create cache dir"() {
        setup:
        ArgumentDelegates.CacheArguments badCacheArguments = Mock()
        // This test assumes this directory does not exist and it will fail to be created
        File cacheDir = new File("/", "genie")

        when:
        new NaiveFetchingCacheService(resourceLoader, badCacheArguments)

        then:
        1 * badCacheArguments.getCacheDirectory() >> cacheDir
        thrown(RuntimeException)
    }

    def "Construct: cache dir does not exist"() {
        setup:
        ArgumentDelegates.CacheArguments badCacheArguments = Mock()

        when:
        new NaiveFetchingCacheService(resourceLoader, badCacheArguments)

        then:
        1 * badCacheArguments.getCacheDirectory() >> new File(temporaryFolder.getRoot(), "genie-cache")
    }

    def "Construct: cache dir is a file"() {
        setup:
        ArgumentDelegates.CacheArguments badCacheArguments = Mock()

        when:
        new NaiveFetchingCacheService(resourceLoader, badCacheArguments)

        then:
        1 * badCacheArguments.getCacheDirectory() >> temporaryFolder.newFile()
        thrown(RuntimeException)
    }
}
