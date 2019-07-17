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
import spock.lang.Specification

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

    def "Resolve successfully"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        Path relativePath = Paths.get("foo/bar.txt")
        URI uri = new URI(AgentFileProtocolResolver.URI_SCHEME, jobId, "/" + relativePath, null)

        String location = uri.toString()
        when:
        Resource r = resolver.resolve(location, resourceLoader)

        then:
        1 * fileStreamService.getResource(jobId, relativePath, uri) >> Optional.of(resource)
        r == resource
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
}
