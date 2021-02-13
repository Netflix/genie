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
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import org.springframework.http.HttpRange
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path
import java.nio.file.Paths

class AgentFileProtocolResolverSpec extends Specification {

    AgentFileStreamService fileStreamService
    AgentFileProtocolResolver resolver
    ResourceLoader resourceLoader
    AgentFileStreamService.AgentFileResource resource

    void setup() {
        this.resource = Mock(AgentFileStreamService.AgentFileResource)
        this.fileStreamService = Mock(AgentFileStreamService)
        this.resourceLoader = Mock(ResourceLoader)

        this.resolver = new AgentFileProtocolResolver(
            fileStreamService
        )
    }

    @Unroll
    def "Resolve successfully with range header #rangeHeader"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        Path relativePath = Paths.get("foo/bar.txt")
        URI uri = AgentFileProtocolResolver.createUri(jobId, "/" + relativePath, rangeHeader)

        String location = uri.toString()
        when:
        Resource r = resolver.resolve(location, resourceLoader)

        then:
        1 * fileStreamService.getResource(jobId, relativePath, uri, httpRange) >> Optional.of(resource)
        r == resource

        where:
        rangeHeader   | httpRange
        null          | null
        ""            | null
        "bytes=10-20" | HttpRange.createByteRange(10, 20)
        "bytes=-20"   | HttpRange.createSuffixRange(20)
        "bytes=20-"   | HttpRange.createByteRange(20)
        "bytes=10-10" | HttpRange.createByteRange(10, 10)
    }

    @Unroll
    def "Reject invalid range header #rangeHeader"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        Path relativePath = Paths.get("foo/bar.txt")
        URI uri = AgentFileProtocolResolver.createUri(jobId, "/" + relativePath, rangeHeader)

        String location = uri.toString()
        when:
        Resource r = resolver.resolve(location, resourceLoader)

        then:
        r == null

        where:
        rangeHeader          | _
        "blah"               | _
        "10-20"              | _
        "-20"                | _
        "20-"                | _
        "lines=10-20"        | _
        "bytes=20-10"        | _
        "bytes=10-20, 30-40" | _
    }

    def "Invalid URI"() {
        when:
        Resource r = resolver.resolve(null, resourceLoader)

        then:
        r == null
    }

    def "Non-agent resource"() {
        setup:
        String location = "http://genie.netflix.com/foo"

        when:
        Resource r = resolver.resolve(location, resourceLoader)

        then:
        r == null
    }

    @Unroll
    def "URI representation for job ID: #jobId path: #path"() {

        when:
        URI uri = AgentFileProtocolResolver.createUri(jobId, path, null)

        then:
        AgentFileProtocolResolver.getAgentResourceURIFilePath(uri) == path
        AgentFileProtocolResolver.getAgentResourceURIFileJobId(uri) == jobId

        where:
        jobId                        | path
        UUID.randomUUID().toString() | ""
        UUID.randomUUID().toString() | "/bar"
        UUID.randomUUID().toString() | "/bar/foo.txt"
        UUID.randomUUID().toString() | "/foo.txt"
        "job_id_1.2.3.4"             | "/foo"
        "job_id with spaces"         | "/foo"
        "\$%*&></+="                 | "/foo"
    }

    @Unroll
    def "URI representation: #path -> #expectedPath"() {

        when:
        URI uri = AgentFileProtocolResolver.createUri(jobId, path, null)

        then:
        AgentFileProtocolResolver.getAgentResourceURIFilePath(uri) == expectedPath
        AgentFileProtocolResolver.getAgentResourceURIFileJobId(uri) == jobId

        where:
        jobId                        | path    | expectedPath
        UUID.randomUUID().toString() | ""      | ""
        UUID.randomUUID().toString() | "/bar"  | "/bar"
        UUID.randomUUID().toString() | "bar"   | "/bar"
        UUID.randomUUID().toString() | "//bar" | "//bar"
    }

    def "URI errors"() {
        setup:
        URI uri = new URI("http://agent/path/to/file")

        when:
        AgentFileProtocolResolver.getAgentResourceURIFilePath(uri)

        then:
        thrown(IllegalArgumentException)

        when:
        AgentFileProtocolResolver.getAgentResourceURIFileJobId(uri)
        then:
        thrown(IllegalArgumentException)
    }
}
