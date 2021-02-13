/*
 *
 *  Copyright 2021 Netflix, Inc.
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
package com.netflix.genie.web.services.impl

import com.netflix.genie.common.internal.jobs.JobConstants
import com.netflix.genie.common.internal.util.GenieHostInfo
import com.netflix.genie.web.properties.JobsForwardingProperties
import org.springframework.http.HttpMethod
import org.springframework.mock.http.client.MockClientHttpRequest
import org.springframework.web.client.RequestCallback
import org.springframework.web.client.RestTemplate
import spock.lang.Specification

import javax.servlet.http.Cookie
import javax.servlet.http.HttpServletRequest

/**
 * Specifications for {@link RequestForwardingServiceImpl}.
 *
 * @author tgianos
 */
@SuppressWarnings("GroovyAccessibility")
class RequestForwardingServiceImplSpec extends Specification {

    RestTemplate restTemplate
    JobsForwardingProperties properties
    GenieHostInfo hostInfo
    String scheme
    String hostname
    int port
    HttpServletRequest request

    RequestForwardingServiceImpl service

    def setup() {
        this.scheme = "https"
        this.port = 8443
        this.hostname = UUID.randomUUID().toString()
        this.hostInfo = Mock(GenieHostInfo) {
            getHostname() >> this.hostname
        }
        this.properties = Mock(JobsForwardingProperties) {
            getPort() >> this.port
            getScheme() >> this.scheme
        }
        this.restTemplate = Mock(RestTemplate)
        this.request = Mock(HttpServletRequest)

        this.service = new RequestForwardingServiceImpl(this.restTemplate, this.hostInfo, this.properties)
    }

    def "can kill"() {
        def destHost = UUID.randomUUID().toString()
        def jobId = UUID.randomUUID().toString()

        when:
        this.service.kill(destHost, jobId, null)

        then:
        1 * this.restTemplate.execute(
            "${this.scheme}://${destHost}:${this.port}/api/v3/jobs/${jobId}",
            HttpMethod.DELETE,
            _ as RequestCallback,
            null,
            []
        )
        noExceptionThrown()

        when:
        this.service.kill(destHost, jobId, this.request)

        then:
        1 * this.restTemplate.execute(
            "${this.scheme}://${destHost}:${this.port}/api/v3/jobs/${jobId}",
            HttpMethod.DELETE,
            _ as RequestCallback,
            null,
            []
        )
        noExceptionThrown()
    }

    def "can't kill"() {
        def destHost = UUID.randomUUID().toString()
        def jobId = UUID.randomUUID().toString()

        when:
        this.service.kill(destHost, jobId, null)

        then:
        1 * this.restTemplate.execute(
            "${this.scheme}://${destHost}:${this.port}/api/v3/jobs/${jobId}",
            HttpMethod.DELETE,
            _ as RequestCallback,
            null,
            []
        ) >> {
            throw new RuntimeException("test")
        }
        thrown(RuntimeException)
    }

    def "can build destination host"() {
        def destHost = UUID.randomUUID().toString()

        expect:
        this.service.buildDestinationHost(destHost) == "${this.scheme}://${destHost}:${this.port}"
    }

    def "can copy request headers"() {
        def header0 = UUID.randomUUID().toString()
        def header1 = UUID.randomUUID().toString()
        def forwardRequest = new MockClientHttpRequest(
            HttpMethod.DELETE,
            URI.create("https://${UUID.randomUUID().toString()}:${8443}${RequestForwardingServiceImpl.JOB_ENDPOINT}")
        )
        def headers = [
            (RequestForwardingServiceImpl.NAME_HEADER_COOKIE): "original header cookie",
            (header0)                                        : UUID.randomUUID().toString(),
            (header1)                                        : UUID.randomUUID().toString()
        ]
        def cookies = [
            new Cookie(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
            new Cookie(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
        ]

        when:
        this.service.copyRequestHeaders(this.request, forwardRequest)

        then:
        1 * this.request.getHeaderNames() >> Collections.enumeration(headers.keySet())
        0 * this.request.getHeader(RequestForwardingServiceImpl.NAME_HEADER_COOKIE)
        1 * this.request.getHeader(header0) >> headers[header0]
        1 * this.request.getHeader(header1) >> headers[header1]
        1 * this.request.getCookies() >> cookies.toArray(new Cookie[0])
        forwardRequest.getHeaders().size() == 4
        forwardRequest.getHeaders().containsKey(header0)
        forwardRequest.getHeaders().get(header0).contains(headers[header0])
        forwardRequest.getHeaders().containsKey(header1)
        forwardRequest.getHeaders().get(header1).contains(headers[header1])
        forwardRequest.getHeaders().containsKey(JobConstants.GENIE_FORWARDED_FROM_HEADER)
        forwardRequest.getHeaders().get(JobConstants.GENIE_FORWARDED_FROM_HEADER).contains(this.hostname)
        forwardRequest.getHeaders().containsKey(RequestForwardingServiceImpl.NAME_HEADER_COOKIE)
        !forwardRequest
            .getHeaders()
            .get(RequestForwardingServiceImpl.NAME_HEADER_COOKIE)
            .contains("original header cookie")
        forwardRequest
            .getHeaders()
            .get(RequestForwardingServiceImpl.NAME_HEADER_COOKIE)
            .contains(cookies.collect { "${it.getName()}=${it.getValue()}" }.join(","))
    }
}
