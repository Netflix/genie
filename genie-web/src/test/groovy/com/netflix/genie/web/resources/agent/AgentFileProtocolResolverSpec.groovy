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

package com.netflix.genie.web.resources.agent

import com.netflix.genie.common.internal.dto.JobDirectoryManifest
import com.netflix.genie.common.internal.util.FileBuffer
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.services.AgentFileManifestService
import com.netflix.genie.web.services.AgentFileStreamService
import org.junit.experimental.categories.Category
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import spock.lang.Specification

import java.nio.file.Paths
import java.time.Instant

@Category(UnitTest)
class AgentFileProtocolResolverSpec extends Specification {

    static final String JOB_ID = "12345"
    static final String PATH = "foo/bar.txt"
    static final int FILE_SIZE = 1000

    AgentFileManifestService manifestService
    AgentFileStreamService fileStreamService
    AgentFileProtocolResolver resolver
    ResourceLoader resourceLoader
    URI uri
    JobDirectoryManifest manifest
    JobDirectoryManifest.ManifestEntry manifestEntry
    FileBuffer fileBuffer
    OutputStream outputStream
    InputStream inputStream
    AgentFileStreamService.ActiveStream activeStream

    void setup() {

        this.manifestService = Mock(AgentFileManifestService)
        this.fileStreamService = Mock(AgentFileStreamService)
        this.resourceLoader = Mock(ResourceLoader)
        this.manifest = Mock(JobDirectoryManifest)
        this.manifestEntry = Mock(JobDirectoryManifest.ManifestEntry)
        this.fileBuffer = Mock(FileBuffer)
        this.outputStream = Mock(OutputStream)
        this.inputStream = Mock(InputStream)
        this.activeStream = Mock(AgentFileStreamService.ActiveStream)

        this.resolver = new AgentFileProtocolResolver(
            manifestService,
            fileStreamService
        )
        
        this.uri = new URI(AgentFileProtocolResolver.URI_SCHEME, JOB_ID, "/" + PATH, null)
    }

    def "Resolve successfully"() {
        setup: 
        String location = uri.toString()
        when:
        Resource r = resolver.resolve(location, resourceLoader)

        then:
        1 * manifestService.getManifest(JOB_ID) >> Optional.of(manifest)
        1 * manifest.getEntry(PATH) >> Optional.of(manifestEntry)
        2 * manifestEntry.getSize() >> FILE_SIZE
        1 * manifestEntry.getName() >> "bar.txt"
        1 * manifestEntry.getLastModifiedTime() >> Instant.now()
        1 * fileStreamService.beginFileStream(JOB_ID, Paths.get(PATH), 0, FILE_SIZE) >> activeStream
        1 * activeStream.getInputStream() >> inputStream

        r != null
        r.class == AgentFileResource
    }

    def "Stream activation exception"() {
        setup:
        String location = uri.toString()
        when:
        Resource r = resolver.resolve(location, resourceLoader)

        then:
        1 * manifestService.getManifest(JOB_ID) >> Optional.of(manifest)
        1 * manifest.getEntry(PATH) >> Optional.of(manifestEntry)
        1 * manifestEntry.getSize() >> FILE_SIZE
        1 * fileStreamService.beginFileStream(JOB_ID, Paths.get(PATH), 0, FILE_SIZE) >> { throw new IOException("...") }
        r == null
    }

    def "Manifest not found"() {
        setup:
        String location = uri.toString()
        when:
        Resource r = resolver.resolve(location, resourceLoader)

        then:
        1 * manifestService.getManifest(JOB_ID) >> Optional.empty()
        r == null
    }

    def "Manifest entry not found"() {
        setup:
        String location = uri.toString()
        when:
        Resource r = resolver.resolve(location, resourceLoader)

        then:
        1 * manifestService.getManifest(JOB_ID) >> Optional.of(manifest)
        1 * manifest.getEntry(PATH) >> Optional.empty()
        r == null
    }
    
    def "Resolve bad location"() {
        when:
        Resource r = resolver.resolve(null, resourceLoader)

        then:
        r == null
    }


}
