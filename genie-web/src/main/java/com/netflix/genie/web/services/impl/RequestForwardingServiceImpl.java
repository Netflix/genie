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
package com.netflix.genie.web.services.impl;

import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.properties.JobsForwardingProperties;
import com.netflix.genie.web.services.RequestForwardingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Nullable;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.util.Enumeration;

/**
 * Default implementation of {@link RequestForwardingService}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class RequestForwardingServiceImpl implements RequestForwardingService {

    private static final String JOB_ENDPOINT = "/api/v3/jobs/";
    private static final String NAME_HEADER_COOKIE = "cookie";

    private final RestTemplate restTemplate;
    private final String hostname;
    private final String apiScheme;
    private final String apiPort;

    /**
     * Constructor.
     *
     * @param restTemplate             The {@link RestTemplate} instance to use to call other Genie nodes API endpoints
     * @param hostInfo                 The {@link GenieHostInfo} instance containing introspection information about
     *                                 the current node
     * @param jobsForwardingProperties The properties related to forwarding requests
     */
    public RequestForwardingServiceImpl(
        final RestTemplate restTemplate,
        final GenieHostInfo hostInfo,
        final JobsForwardingProperties jobsForwardingProperties
    ) {
        this.restTemplate = restTemplate;
        this.hostname = hostInfo.getHostname();
        this.apiScheme = jobsForwardingProperties.getScheme() + "://";
        this.apiPort = ":" + jobsForwardingProperties.getPort();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    // TODO: Enable retries?
    public void kill(final String host, final String jobId, @Nullable final HttpServletRequest request) {
        final String endpoint = this.buildDestinationHost(host) + JOB_ENDPOINT + jobId;
        log.info("Attempting to forward kill request by calling DELETE at {}", endpoint);
        try {
            this.restTemplate.execute(
                endpoint,
                HttpMethod.DELETE,
                forwardRequest -> {
                    forwardRequest.getHeaders().add(JobConstants.GENIE_FORWARDED_FROM_HEADER, this.hostname);
                    if (request != null) {
                        this.copyRequestHeaders(request, forwardRequest);
                    }
                },
                null
            );
        } catch (final Exception e) {
            log.error("Failed sending DELETE to {}. Error: {}", endpoint, e.getMessage(), e);
            throw e;
        }
    }

    private String buildDestinationHost(final String destinationHost) {
        return this.apiScheme + destinationHost + this.apiPort;
    }

    /*
     * Copied from legacy code in JobRestController.
     */
    private void copyRequestHeaders(final HttpServletRequest request, final ClientHttpRequest forwardRequest) {
        // Copy all the headers (necessary for ACCEPT and security headers especially). Do not copy the cookie header.
        final HttpHeaders headers = forwardRequest.getHeaders();
        final Enumeration<String> headerNames = request.getHeaderNames();
        if (headerNames != null) {
            while (headerNames.hasMoreElements()) {
                final String headerName = headerNames.nextElement();
                if (!NAME_HEADER_COOKIE.equals(headerName)) {
                    final String headerValue = request.getHeader(headerName);
                    log.debug("Request Header: name = {} value = {}", headerName, headerValue);
                    headers.add(headerName, headerValue);
                }
            }
        }
        // Lets add the cookie as an header
        final Cookie[] cookies = request.getCookies();
        if (cookies != null && cookies.length > 0) {
            final StringBuilder builder = new StringBuilder();
            for (final Cookie cookie : cookies) {
                if (builder.length() != 0) {
                    builder.append(",");
                }
                builder.append(cookie.getName()).append("=").append(cookie.getValue());
            }
            final String cookieValue = builder.toString();
            headers.add(NAME_HEADER_COOKIE, cookieValue);
            log.debug("Request Header: name = {} value = {}", NAME_HEADER_COOKIE, cookieValue);
        }
        forwardRequest.getHeaders().add(JobConstants.GENIE_FORWARDED_FROM_HEADER, this.hostname);
    }
}
