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

import com.netflix.genie.agent.cli.ArgumentDelegates
import com.netflix.genie.agent.execution.exceptions.DownloadException
import com.netflix.genie.agent.utils.locks.CloseableLock
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory
import com.netflix.genie.test.categories.UnitTest
import org.apache.commons.lang3.tuple.Pair
import org.assertj.core.util.Sets
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import spock.lang.Shared
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.util.concurrent.locks.ReentrantLock

@Category(UnitTest.class)
class FetchingCacheServiceImplSpec extends Specification {

    ResourceLoader resourceLoader
    ArgumentDelegates.CacheArguments cacheArguments
    FetchingCacheServiceImpl cache
    Resource resource
    long DEFAULT_RESOURCE_LAST_MODIFIED_TS = 100
    URI uri
    FileLockFactory fileLockFactory;
    CloseableLock lock;
    ReentrantLock reentrantLock = new ReentrantLock()

    @Rule
    TemporaryFolder temporaryFolder

    @Shared
    def fetchingCacheServiceCleanUpTaskExecutor = new ThreadPoolTaskExecutor()

    def setupSpec() {
        this.fetchingCacheServiceCleanUpTaskExecutor.setCorePoolSize(1)
        this.fetchingCacheServiceCleanUpTaskExecutor.initialize()
    }

    def cleanupSpec() {
        this.fetchingCacheServiceCleanUpTaskExecutor.shutdown()
    }

    void setup() {
        temporaryFolder.create()
        resourceLoader = Mock()
        cacheArguments = Mock()
        cacheArguments.getCacheDirectory() >> temporaryFolder.getRoot()
        resource = Mock()
        fileLockFactory = Mock();
        lock = Mock()
        fileLockFactory.getLock(_ as File) >> lock
        lock.lock() >> reentrantLock.lock()
        lock.close() >> reentrantLock.unlock()
        cache = new FetchingCacheServiceImpl(resourceLoader, cacheArguments, fileLockFactory, fetchingCacheServiceCleanUpTaskExecutor)
        uri = new URI("https://my-server.com/path/to/config/config.xml")
    }

    def "Get resource not cached. Get resources cached"() {
        setup:
        String fileContents = "example file contents\n"
        File targetFile = new File(temporaryFolder.getRoot(), "target")
        File cachedFile = cache.getCacheResourceVersionDataFile(cache.getResourceCacheId(uri), DEFAULT_RESOURCE_LAST_MODIFIED_TS)
        File lockFile = cache.getCacheResourceVersionLockFile(cache.getResourceCacheId(uri), DEFAULT_RESOURCE_LAST_MODIFIED_TS)
        File downloadDataFile = cache.getCacheResourceVersionDownloadFile(cache.getResourceCacheId(uri), DEFAULT_RESOURCE_LAST_MODIFIED_TS)

        when:
        cache.get(uri, targetFile)

        then:
        1 * resource.exists() >> true
        1 * resource.lastModified() >> DEFAULT_RESOURCE_LAST_MODIFIED_TS
        1 * resourceLoader.getResource(_ as String) >> resource
        1 * resource.getInputStream() >> new ByteArrayInputStream(fileContents.getBytes())
        cachedFile.exists()
        cachedFile.getText(StandardCharsets.UTF_8.toString()) == fileContents
        targetFile.exists()
        targetFile.getText(StandardCharsets.UTF_8.toString()) == fileContents
        !downloadDataFile.exists()
        lockFile.exists()

        when:
        cache.get(uri, targetFile)

        then:
        1 * resource.exists() >> true
        1 * resource.lastModified() >> DEFAULT_RESOURCE_LAST_MODIFIED_TS
        0 * resource.getInputStream()
        1 * resourceLoader.getResource(_ as String) >> resource
        targetFile.exists()
        targetFile.getText(StandardCharsets.UTF_8.toString()) == fileContents
    }

    def "Download new version, delete previous version"() {
        setup:
        String fileContents = "example file contents\n"
        File targetFile = new File(temporaryFolder.getRoot(), "target")
        File cachedFile = cache.getCacheResourceVersionDataFile(cache.getResourceCacheId(uri), DEFAULT_RESOURCE_LAST_MODIFIED_TS)
        File downloadDataFile = cache.getCacheResourceVersionDownloadFile(cache.getResourceCacheId(uri), DEFAULT_RESOURCE_LAST_MODIFIED_TS)
        File lockFile = cache.getCacheResourceVersionLockFile(cache.getResourceCacheId(uri), DEFAULT_RESOURCE_LAST_MODIFIED_TS)
        File resourceVersionDirectory = cache.getCacheResourceVersionDir(cache.getResourceCacheId(uri), DEFAULT_RESOURCE_LAST_MODIFIED_TS)
        File targetFile2 = new File(temporaryFolder.getRoot(), "target2")
        long newerVersionLastModifiedTimestamp = DEFAULT_RESOURCE_LAST_MODIFIED_TS + 1
        File cachedFile2 = cache.getCacheResourceVersionDataFile(cache.getResourceCacheId(uri), newerVersionLastModifiedTimestamp)
        File lockFile2 = cache.getCacheResourceVersionLockFile(cache.getResourceCacheId(uri), newerVersionLastModifiedTimestamp)
        File downloadDataFile2 = cache.getCacheResourceVersionDownloadFile(cache.getResourceCacheId(uri), newerVersionLastModifiedTimestamp)

        when:
        cache.get(uri, targetFile)

        then:
        1*resource.exists() >> true
        1*resource.lastModified() >> DEFAULT_RESOURCE_LAST_MODIFIED_TS
        1*resource.getInputStream() >> new ByteArrayInputStream(fileContents.getBytes())
        1*resourceLoader.getResource(_ as String) >> resource
        cachedFile.exists()
        cachedFile.getText(StandardCharsets.UTF_8.toString()) == fileContents
        targetFile.exists()
        targetFile.getText(StandardCharsets.UTF_8.toString()) == fileContents


        when:
        cache.get(uri, targetFile2)

        then:
        1*resource.exists() >> true
        1*resource.lastModified() >> DEFAULT_RESOURCE_LAST_MODIFIED_TS + 1
        1*resource.getInputStream() >> new ByteArrayInputStream(fileContents.getBytes())
        1*resourceLoader.getResource(_ as String) >> resource
        sleep(2000) //since deletion happens on a separate thread
        resourceVersionDirectory.exists()
        !cachedFile.exists()
        !downloadDataFile.exists()
        lockFile.exists()
        targetFile.exists()
        targetFile.getText(StandardCharsets.UTF_8.toString()) == fileContents
        cachedFile2.exists()
        cachedFile2.getText(StandardCharsets.UTF_8.toString()) == fileContents
        targetFile2.exists()
        targetFile2.getText(StandardCharsets.UTF_8.toString()) == fileContents
        !downloadDataFile2.exists()
        lockFile2.exists()

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

        when:
        cache.get(uri, targetFile)

        then:
        1 * resourceLoader.getResource(_ as String) >> resource
        1 * resource.exists() >> true
        1 * resource.lastModified() >> DEFAULT_RESOURCE_LAST_MODIFIED_TS
        1 * resource.getInputStream() >> null
        thrown(IllegalArgumentException)
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
        1 * resources[0].lastModified() >> DEFAULT_RESOURCE_LAST_MODIFIED_TS
        1 * resources[1].lastModified() >> DEFAULT_RESOURCE_LAST_MODIFIED_TS
        1 * resources[2].lastModified() >> DEFAULT_RESOURCE_LAST_MODIFIED_TS
        1 * resources[0].getInputStream() >> new ByteArrayInputStream(uris[0].toString().getBytes(StandardCharsets.UTF_8))
        1 * resources[1].getInputStream() >> new ByteArrayInputStream(uris[1].toString().getBytes(StandardCharsets.UTF_8))
        1 * resources[2].getInputStream() >> new ByteArrayInputStream(uris[2].toString().getBytes(StandardCharsets.UTF_8))
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
        new FetchingCacheServiceImpl(resourceLoader, badCacheArguments, fileLockFactory, fetchingCacheServiceCleanUpTaskExecutor)

        then:
        1 * badCacheArguments.getCacheDirectory() >> cacheDir
        thrown(IOException)
    }

    def "Construct: create a dir succesfully"() {
        setup:
        ArgumentDelegates.CacheArguments goodCacheArguments = Mock()
        File cacheDir = new File(temporaryFolder.getRoot(), "genie-cache")

        when:
        new FetchingCacheServiceImpl(resourceLoader, goodCacheArguments, fileLockFactory, fetchingCacheServiceCleanUpTaskExecutor)

        then:
        1 * goodCacheArguments.getCacheDirectory() >> cacheDir
        cacheDir.exists()
    }

    def "Construct: cache dir is a file"() {
        setup:
        ArgumentDelegates.CacheArguments badCacheArguments = Mock()

        when:
        new FetchingCacheServiceImpl(resourceLoader, badCacheArguments, fileLockFactory, fetchingCacheServiceCleanUpTaskExecutor)

        then:
        1 * badCacheArguments.getCacheDirectory() >> temporaryFolder.newFile()
        thrown(IOException)
    }
}
