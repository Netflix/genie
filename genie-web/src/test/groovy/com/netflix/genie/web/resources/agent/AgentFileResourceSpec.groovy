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
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.services.AgentFileStreamService
import org.junit.experimental.categories.Category
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant

@Category(UnitTest)
class AgentFileResourceSpec extends Specification {

    String fileName = "bar.txt"
    Instant modTime = Instant.now()
    long size = 1000
    String jobId = "123456"
    Path path = Paths.get(".")

    AgentFileStreamService.ActiveStream fileStream
    JobDirectoryManifest.ManifestEntry manifestEntry
    InputStream inputStream
    URI uri

    void setup() {
        this.fileStream = Mock(AgentFileStreamService.ActiveStream)
        this.manifestEntry = Mock(JobDirectoryManifest.ManifestEntry)
        this.inputStream = Mock(InputStream)
        this.uri = new URI(AgentFileProtocolResolver.URI_SCHEME, jobId, "/" + path, null)
    }

    def "Construct" () {
        setup:

        when:
        AgentFileResource resource = new AgentFileResource(uri, fileStream, manifestEntry)

        then:
        1 * fileStream.getInputStream() >> inputStream
        1 * manifestEntry.getName() >> fileName
        1 * manifestEntry.getLastModifiedTime() >> modTime
        1 * manifestEntry.getSize() >> size
        1 * fileStream.getJobId() >> jobId
        1 * fileStream.getRelativePath() >> path

        resource.exists()
        resource.isReadable()
        resource.isOpen()
        !resource.isFile()
        resource.getURI() == uri
        resource.contentLength() == size
        resource.lastModified() == modTime.toEpochMilli()
        resource.getFilename() == fileName
        resource.getDescription().contains(jobId)
        resource.getDescription().contains(path.toString())
        resource.getInputStream() == inputStream

        when:
        resource.getFile()

        then:
        thrown(FileNotFoundException)

        when:
        resource.createRelative("...")

        then:
        thrown(IOException)

        when:
        resource.getURL()

        then:
        thrown(IOException)
    }
}
