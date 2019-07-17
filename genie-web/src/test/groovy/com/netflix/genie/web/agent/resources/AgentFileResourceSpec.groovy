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
package com.netflix.genie.web.agent.resources

import com.netflix.genie.web.agent.services.AgentFileStreamService
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant

class AgentFileResourceSpec extends Specification {

    long size = 1000
    String jobId = "123456"
    Path path = Paths.get("foo/bar.txt")

    InputStream inputStream = Mock(InputStream)
    URI uri = new URI(AgentFileProtocolResolver.URI_SCHEME, jobId, "/" + path, null)
    long lastModTime = 1553793121 * 1000

    def "Construct for resource"() {

        when:
        AgentFileStreamService.AgentFileResource resource = AgentFileResourceImpl.forAgentFile(
            uri,
            size,
            Instant.ofEpochMilli(lastModTime),
            path,
            jobId,
            inputStream
        )

        then:
        resource != null
        resource.exists()
        resource.isReadable()
        resource.isOpen()
        !resource.isFile()
        resource.getFilename() == "bar.txt"
        resource.getDescription().contains(jobId)
        resource.getDescription().contains(path.toString())
        resource.getURI() == uri
        resource.contentLength() == size
        resource.lastModified() == lastModTime
        resource.getInputStream() == inputStream
        resource.readableChannel() != null

        when:
        resource.getURL()

        then:
        thrown(IOException)

        when:
        resource.getFile()

        then:
        thrown(IOException)

        when:
        resource.createRelative()

        then:
        thrown(IOException)

    }

    def "Construct for non-existing"() {

        when:
        AgentFileStreamService.AgentFileResource resource = AgentFileResourceImpl.forNonExistingResource()

        then:
        resource != null
        !resource.exists()
        !resource.isReadable()
        resource.isOpen()
        !resource.isFile()
        resource.getFilename() == null
        resource.getDescription().contains("non-existent")

        when:
        resource.getURI()

        then:
        thrown(IOException)

        when:
        resource.getURL()

        then:
        thrown(IOException)

        when:
        resource.getFile()

        then:
        thrown(IOException)

        when:
        resource.contentLength()

        then:
        thrown(IOException)

        when:
        resource.lastModified()

        then:
        thrown(IOException)

        when:
        resource.createRelative()

        then:
        thrown(IOException)

        when:
        resource.getInputStream()
        then:
        thrown(FileNotFoundException)

        when:
        resource.readableChannel()

        then:
        thrown(FileNotFoundException)
    }
}
