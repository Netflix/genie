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
package com.netflix.genie.common.internal.services.impl

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Ticker
import com.netflix.genie.common.internal.dtos.DirectoryManifest
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

/**
 * Unit test for JobDirectoryManifestServiceImplSpec
 */
class JobDirectoryManifestCreatorServiceImplSpec extends Specification {
    DirectoryManifest.Factory factory
    Cache<Path, DirectoryManifest> cache

    static IOException ioe = new IOException("...")
    static RuntimeException re = new RuntimeException("...")
    static IllegalStateException ise = new IllegalStateException("...")
    static InterruptedException ie = new InterruptedException("...")

    void setup() {
        this.factory = Mock(DirectoryManifest.Factory)
        this.cache = Caffeine.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build()
    }

    @Unroll
    def "GetDirectoryManifest cache (checksum: #includeChecksum)"() {
        setup:
        JobDirectoryManifestCreatorService service = new JobDirectoryManifestCreatorServiceImpl(factory, cache, includeChecksum)

        when:
        for (int i = 0; i < 10; i++) {
            service.getDirectoryManifest(Paths.get("/temp/foo"))
        }

        then:
        1 * factory.getDirectoryManifest(Paths.get("/temp/foo"), includeChecksum) >> Mock(DirectoryManifest)

        where:
        includeChecksum | _
        true            | _
        false           | _
    }

    @Unroll
    def "GetDirectoryManifest loading exception: #factoryException"(
        Exception factoryException,
        Exception expectedException,
        Class<? extends Exception> expectedExceptionType
    ) {
        setup:
        boolean includeChecksum = false
        JobDirectoryManifestCreatorService service = new JobDirectoryManifestCreatorServiceImpl(factory, cache, includeChecksum)

        when:
        service.getDirectoryManifest(Paths.get("/temp/foo"))

        then:
        1 * factory.getDirectoryManifest(Paths.get("/temp/foo"), includeChecksum) >> { throw factoryException }
        def t = thrown(Exception)
        expectedExceptionType.isInstance(t)
        if (expectedException != null) {
            t == expectedException
        }

        where:
        factoryException | expectedException | expectedExceptionType
        ioe              | ioe               | IOException
        re               | re                | RuntimeException
        ise              | null              | RuntimeException
        ie               | null              | InterruptedException
    }

    def "GetDirectoryManifest cache time-based eviction"() {
        setup:
        def ticker = Mock(Ticker)
        def cache = Caffeine.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .ticker(ticker)
            .build()
        JobDirectoryManifestCreatorService service = new JobDirectoryManifestCreatorServiceImpl(factory, cache, false)

        when:
        for (int i = 0; i < 10; i++) {
            service.getDirectoryManifest(Paths.get("/temp/foo"))
        }

        then:
        1 * factory.getDirectoryManifest(Paths.get("/temp/foo"), false) >> Mock(DirectoryManifest)
        _ * ticker.read() >> 0

        when:
        service.invalidateCachedDirectoryManifest(Paths.get("/temp/foo"))
        service.getDirectoryManifest(Paths.get("/temp/foo"))

        then:
        1 * factory.getDirectoryManifest(Paths.get("/temp/foo"), false) >> Mock(DirectoryManifest)
        _ * ticker.read() >> 0

        when:
        service.getDirectoryManifest(Paths.get("/temp/foo"))

        then:
        _ * ticker.read() >> TimeUnit.HOURS.toNanos(1) + 1
        1 * factory.getDirectoryManifest(Paths.get("/temp/foo"), false) >> Mock(DirectoryManifest)
    }
}
