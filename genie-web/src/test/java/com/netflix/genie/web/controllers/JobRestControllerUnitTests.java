/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.controllers;

import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.AttachmentService;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.hateoas.assemblers.JobResourceAssembler;
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import org.apache.catalina.ssi.ByteArrayServletOutputStream;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for the Job rest controller.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobRestControllerUnitTests {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    //Mocked variables
    private JobCoordinatorService jobService;
    private AttachmentService attachmentService;
    private JobResourceAssembler jobResourceAssembler;
    private String hostname;
    private HttpClient httpClient;
    private GenieResourceHttpRequestHandler genieResourceHttpRequestHandler;

    private JobRestController controller;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobService = Mockito.mock(JobCoordinatorService.class);
        this.attachmentService = Mockito.mock(AttachmentService.class);
        this.jobResourceAssembler = Mockito.mock(JobResourceAssembler.class);
        this.hostname = UUID.randomUUID().toString();
        this.httpClient = Mockito.mock(HttpClient.class);
        this.genieResourceHttpRequestHandler = Mockito.mock(GenieResourceHttpRequestHandler.class);

        this.controller = new JobRestController(
            this.jobService,
            this.attachmentService,
            this.jobResourceAssembler,
            this.hostname,
            this.httpClient,
            this.genieResourceHttpRequestHandler,
            true
        );
    }

    /**
     * Make sure if directory forwarding isn't enabled it never fires.
     *
     * @throws IOException      on error
     * @throws ServletException on error
     * @throws GenieException   on error
     */
    @Test
    public void wontForwardJobOutputRequestIfNotEnabled() throws IOException, ServletException, GenieException {
        this.controller = new JobRestController(
            this.jobService,
            this.attachmentService,
            this.jobResourceAssembler,
            this.hostname,
            this.httpClient,
            this.genieResourceHttpRequestHandler,
            false
        );

        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.doNothing().when(this.genieResourceHttpRequestHandler).handleRequest(request, response);

        this.controller.getJobOutput(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobService, Mockito.never()).getJobHost(Mockito.eq(jobId));
        Mockito.verify(this.genieResourceHttpRequestHandler, Mockito.times(1)).handleRequest(request, response);
    }

    /**
     * Make sure if directory forwarding doesn't fire if already forwarded.
     *
     * @throws IOException      on error
     * @throws ServletException on error
     * @throws GenieException   on error
     */
    @Test
    public void wontForwardJobOutputRequestIfAlreadyForwarded() throws IOException, ServletException, GenieException {
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = UUID.randomUUID().toString();
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.doNothing().when(this.genieResourceHttpRequestHandler).handleRequest(request, response);

        this.controller.getJobOutput(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobService, Mockito.never()).getJobHost(Mockito.eq(jobId));
        Mockito.verify(this.genieResourceHttpRequestHandler, Mockito.times(1)).handleRequest(request, response);
    }

    /**
     * Make sure if directory forwarding doesn't fire if host name matches.
     *
     * @throws IOException      on error
     * @throws ServletException on error
     * @throws GenieException   on error
     */
    @Test
    public void wontForwardJobOutputRequestIfOnCorrectHost() throws IOException, ServletException, GenieException {
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.doNothing().when(this.genieResourceHttpRequestHandler).handleRequest(request, response);

        Mockito.when(this.jobService.getJobHost(jobId)).thenReturn(this.hostname);

        this.controller.getJobOutput(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobService, Mockito.times(1)).getJobHost(Mockito.eq(jobId));
        Mockito.verify(this.httpClient, Mockito.never()).execute(Mockito.any());
        Mockito.verify(this.genieResourceHttpRequestHandler, Mockito.times(1)).handleRequest(request, response);
    }

    /**
     * Make sure directory forwarding happens when all conditions are met.
     *
     * @throws IOException      on error
     * @throws ServletException on error
     * @throws GenieException   on error
     */
    @Test
    public void canHandleForwardJobOutputRequestWithError() throws IOException, ServletException, GenieException {
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.doNothing().when(this.genieResourceHttpRequestHandler).handleRequest(request, response);

        final String jobHostName = UUID.randomUUID().toString();
        Mockito.when(this.jobService.getJobHost(jobId)).thenReturn(jobHostName);

        //Mock parts of the http request
        final String http = "http";
        Mockito.when(request.getScheme()).thenReturn(http);
        final int port = 8080;
        Mockito.when(request.getServerPort()).thenReturn(port);
        final String requestURI = "/" + jobId + "/" + UUID.randomUUID().toString();
        Mockito.when(request.getRequestURI()).thenReturn(requestURI);
        Mockito.when(request.getHeaderNames()).thenReturn(null);

        final String requestUrl = UUID.randomUUID().toString();
        Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer(requestUrl));

        //Mock parts of forward response
        final HttpResponse forwardResponse = Mockito.mock(HttpResponse.class);
        final StatusLine statusLine = Mockito.mock(StatusLine.class);
        Mockito.when(forwardResponse.getStatusLine()).thenReturn(statusLine);
        final int errorCode = 404;
        Mockito.when(statusLine.getStatusCode()).thenReturn(errorCode);
        Mockito.when(this.httpClient.execute(Mockito.any())).thenReturn(forwardResponse);

        this.controller.getJobOutput(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobService, Mockito.times(1)).getJobHost(Mockito.eq(jobId));
        Mockito.verify(this.httpClient, Mockito.times(1)).execute(Mockito.any());
        Mockito.verify(response, Mockito.times(1)).sendError(errorCode);
        Mockito.verify(this.genieResourceHttpRequestHandler, Mockito.never()).handleRequest(request, response);
    }

    /**
     * Make sure directory forwarding happens when all conditions are met.
     *
     * @throws IOException      on error
     * @throws ServletException on error
     * @throws GenieException   on error
     */
    @Test
    public void canHandleForwardJobOutputRequestWithSuccess() throws IOException, ServletException, GenieException {
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.doNothing().when(this.genieResourceHttpRequestHandler).handleRequest(request, response);

        final String jobHostName = UUID.randomUUID().toString();
        Mockito.when(this.jobService.getJobHost(jobId)).thenReturn(jobHostName);

        //Mock parts of the http request
        final String http = "http";
        Mockito.when(request.getScheme()).thenReturn(http);
        final int port = 8080;
        Mockito.when(request.getServerPort()).thenReturn(port);
        final String requestURI = "/" + jobId + "/" + UUID.randomUUID().toString();
        Mockito.when(request.getRequestURI()).thenReturn(requestURI);

        final Set<String> headerNames = Sets.newHashSet(HttpHeaders.ACCEPT);
        Mockito.when(request.getHeaderNames()).thenReturn(Collections.enumeration(headerNames));
        Mockito.when(request.getHeader(HttpHeaders.ACCEPT)).thenReturn(MediaType.APPLICATION_JSON_VALUE);

        final String requestUrl = UUID.randomUUID().toString();
        Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer(requestUrl));

        //Mock parts of forward response
        final HttpResponse forwardResponse = Mockito.mock(HttpResponse.class);
        final StatusLine statusLine = Mockito.mock(StatusLine.class);
        Mockito.when(forwardResponse.getStatusLine()).thenReturn(statusLine);
        final int successCode = 200;
        Mockito.when(statusLine.getStatusCode()).thenReturn(successCode);
        final Header contentTypeHeader = Mockito.mock(Header.class);
        Mockito.when(contentTypeHeader.getName()).thenReturn(HttpHeaders.CONTENT_TYPE);
        Mockito.when(contentTypeHeader.getValue()).thenReturn(MediaType.TEXT_PLAIN_VALUE);
        Mockito.when(forwardResponse.getAllHeaders()).thenReturn(new Header[]{contentTypeHeader});

        final String text = UUID.randomUUID().toString() + UUID.randomUUID().toString() + UUID.randomUUID().toString();
        final ByteArrayInputStream bis = new ByteArrayInputStream(text.getBytes(UTF_8));
        final HttpEntity entity = Mockito.mock(HttpEntity.class);
        Mockito.when(entity.getContent()).thenReturn(bis);
        Mockito.when(forwardResponse.getEntity()).thenReturn(entity);

        final ByteArrayServletOutputStream bos = new ByteArrayServletOutputStream();
        Mockito.when(response.getOutputStream()).thenReturn(bos);

        Mockito.when(this.httpClient.execute(Mockito.any())).thenReturn(forwardResponse);

        this.controller.getJobOutput(jobId, forwardedFrom, request, response);

        Assert.assertThat(new String(bos.toByteArray(), UTF_8), Matchers.is(text));
        Mockito.verify(request, Mockito.times(1)).getHeader(HttpHeaders.ACCEPT);
        Mockito.verify(this.jobService, Mockito.times(1)).getJobHost(Mockito.eq(jobId));
        Mockito.verify(this.httpClient, Mockito.times(1)).execute(Mockito.any());
        Mockito.verify(response, Mockito.never()).sendError(Mockito.anyInt());
        Mockito.verify(response, Mockito.times(1)).setHeader(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN_VALUE);
        Mockito.verify(this.genieResourceHttpRequestHandler, Mockito.never()).handleRequest(request, response);
    }
}
