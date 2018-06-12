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
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.hateoas.assemblers.ApplicationResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.ClusterResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.CommandResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobExecutionResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobMetadataResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobRequestResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobSearchResultResourceAssembler;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.JobCoordinatorService;
import com.netflix.genie.web.services.JobSearchService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.catalina.ssi.ByteArrayServletOutputStream;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.mock.http.client.MockClientHttpResponse;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

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
    private JobSearchService jobSearchService;
    private String hostname;
    private RestTemplate restTemplate;
    private GenieResourceHttpRequestHandler genieResourceHttpRequestHandler;
    private JobsProperties jobsProperties;

    private JobRestController controller;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobSearchService = Mockito.mock(JobSearchService.class);
        this.hostname = UUID.randomUUID().toString();
        this.restTemplate = Mockito.mock(RestTemplate.class);
        this.genieResourceHttpRequestHandler = Mockito.mock(GenieResourceHttpRequestHandler.class);
        this.jobsProperties = new JobsProperties();

        final MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        final Counter counter = Mockito.mock(Counter.class);
        Mockito.when(registry.counter(Mockito.anyString())).thenReturn(counter);

        this.controller = new JobRestController(
            Mockito.mock(JobCoordinatorService.class),
            this.jobSearchService,
            Mockito.mock(AttachmentService.class),
            Mockito.mock(ApplicationResourceAssembler.class),
            Mockito.mock(ClusterResourceAssembler.class),
            Mockito.mock(CommandResourceAssembler.class),
            Mockito.mock(JobResourceAssembler.class),
            Mockito.mock(JobRequestResourceAssembler.class),
            Mockito.mock(JobExecutionResourceAssembler.class),
            Mockito.mock(JobMetadataResourceAssembler.class),
            Mockito.mock(JobSearchResultResourceAssembler.class),
            new GenieHostInfo(this.hostname),
            this.restTemplate,
            this.genieResourceHttpRequestHandler,
            this.jobsProperties,
            registry
        );
    }

    /**
     * Make sure if forwarding isn't enabled we don't even try to forward no matter where the job is running.
     *
     * @throws IOException      On error
     * @throws ServletException On Error
     * @throws GenieException   On Error
     */
    @Test
    public void wontForwardKillRequestIfNotEnabled() throws IOException, ServletException, GenieException {
        this.jobsProperties.getForwarding().setEnabled(false);

        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        this.controller.killJob(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobSearchService, Mockito.never()).getJobHost(jobId);
    }

    /**
     * Make sure won't forward job request if it's already been forwarded.
     *
     * @throws IOException      on error
     * @throws ServletException on error
     * @throws GenieException   on error
     */
    @Test
    public void wontForwardJobKillRequestIfAlreadyForwarded() throws IOException, ServletException, GenieException {
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = UUID.randomUUID().toString();
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        this.controller.killJob(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobSearchService, Mockito.never()).getJobHost(Mockito.eq(jobId));
    }

    /**
     * Makes sure we don't forward the request if we're already on the right host.
     *
     * @throws IOException      on error
     * @throws ServletException on error
     * @throws GenieException   on error
     */
    @Test
    public void wontForwardJobKillRequestIfOnCorrectHost() throws IOException, ServletException, GenieException {
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.when(this.jobSearchService.getJobHost(jobId)).thenReturn(this.hostname);

        this.controller.killJob(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobSearchService, Mockito.times(1)).getJobHost(jobId);
        Mockito
            .verify(this.restTemplate, Mockito.never())
            .execute(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    /**
     * Makes sure if we do forward and get back an error we return it to the user.
     *
     * @throws IOException      on error
     * @throws ServletException on error
     * @throws GenieException   on error
     */
    @Test
    public void canRespondToKillRequestForwardError() throws IOException, ServletException, GenieException {
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer(UUID.randomUUID().toString()));
        Mockito.when(this.jobSearchService.getJobHost(jobId)).thenReturn(UUID.randomUUID().toString());

        final StatusLine statusLine = Mockito.mock(StatusLine.class);
        Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.NOT_FOUND.value());
        final HttpResponse forwardResponse = Mockito.mock(HttpResponse.class);
        Mockito.when(forwardResponse.getStatusLine()).thenReturn(statusLine);
        Mockito.when(
            this.restTemplate.execute(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyString()
            )
        )
            .thenThrow(new HttpClientErrorException(HttpStatus.NOT_FOUND));

        this.controller.killJob(jobId, forwardedFrom, request, response);

        Mockito
            .verify(response, Mockito.times(1))
            .sendError(Mockito.eq(HttpStatus.NOT_FOUND.value()), Mockito.anyString());
        Mockito.verify(this.jobSearchService, Mockito.times(1)).getJobHost(jobId);
        Mockito
            .verify(this.restTemplate, Mockito.times(1))
            .execute(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyString()
            );
    }

    /**
     * Makes sure we can successfully forward a job kill request.
     *
     * @throws IOException      on error
     * @throws ServletException on error
     * @throws GenieException   on error
     */
    @Test
    public void canForwardJobKillRequest() throws IOException, ServletException, GenieException {
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer(UUID.randomUUID().toString()));
        Mockito.when(this.jobSearchService.getJobHost(jobId)).thenReturn(UUID.randomUUID().toString());

        final StatusLine statusLine = Mockito.mock(StatusLine.class);
        Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.ACCEPTED.value());
        final HttpResponse forwardResponse = Mockito.mock(HttpResponse.class);
        Mockito.when(forwardResponse.getStatusLine()).thenReturn(statusLine);
        Mockito.when(forwardResponse.getAllHeaders()).thenReturn(new Header[0]);
        Mockito
            .when(
                this.restTemplate.execute(
                    Mockito.anyString(),
                    Mockito.any(),
                    Mockito.any(),
                    Mockito.any(),
                    Mockito.anyString()
                )
            )
            .thenReturn(null);

        this.controller.killJob(jobId, forwardedFrom, request, response);

        Mockito.verify(response, Mockito.never()).sendError(Mockito.anyInt(), Mockito.anyString());
        Mockito.verify(this.jobSearchService, Mockito.times(1)).getJobHost(jobId);
        Mockito.verify(this.restTemplate, Mockito.times(1))
            .execute(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString());
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
        this.jobsProperties.getForwarding().setEnabled(false);

        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.doNothing().when(this.genieResourceHttpRequestHandler).handleRequest(request, response);

        this.controller.getJobOutput(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobSearchService, Mockito.never()).getJobHost(Mockito.eq(jobId));
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
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = UUID.randomUUID().toString();
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.doNothing().when(this.genieResourceHttpRequestHandler).handleRequest(request, response);

        this.controller.getJobOutput(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobSearchService, Mockito.never()).getJobHost(Mockito.eq(jobId));
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
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.doNothing().when(this.genieResourceHttpRequestHandler).handleRequest(request, response);

        Mockito.when(this.jobSearchService.getJobHost(jobId)).thenReturn(this.hostname);

        this.controller.getJobOutput(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobSearchService, Mockito.times(1)).getJobHost(Mockito.eq(jobId));
        Mockito
            .verify(this.restTemplate, Mockito.never())
            .execute(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyString(),
                Mockito.anyString()
            );
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
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.doNothing().when(this.genieResourceHttpRequestHandler).handleRequest(request, response);

        final String jobHostName = UUID.randomUUID().toString();
        Mockito.when(this.jobSearchService.getJobHost(jobId)).thenReturn(jobHostName);

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

        final int errorCode = 404;
        Mockito.when(
            this.restTemplate.execute(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyString(),
                Mockito.anyString()
            )
        )
            .thenThrow(new HttpClientErrorException(HttpStatus.NOT_FOUND));

        this.controller.getJobOutput(jobId, forwardedFrom, request, response);

        Mockito.verify(this.jobSearchService, Mockito.times(1)).getJobHost(Mockito.eq(jobId));
        Mockito.verify(this.restTemplate, Mockito.times(1))
            .execute(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyString(),
                Mockito.anyString()
            );
        Mockito.verify(response, Mockito.times(1)).sendError(Mockito.eq(errorCode), Mockito.anyString());
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
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = null;
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.doNothing().when(this.genieResourceHttpRequestHandler).handleRequest(request, response);

        final String jobHostName = UUID.randomUUID().toString();
        Mockito.when(this.jobSearchService.getJobHost(jobId)).thenReturn(jobHostName);

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

        final ClientHttpRequestFactory factory = Mockito.mock(ClientHttpRequestFactory.class);
        final ClientHttpRequest clientHttpRequest = Mockito.mock(ClientHttpRequest.class);
        Mockito.when(clientHttpRequest.execute())
            .thenReturn(new MockClientHttpResponse(text.getBytes(UTF_8), HttpStatus.OK));
        Mockito.when(clientHttpRequest.getHeaders())
            .thenReturn(new HttpHeaders());
        Mockito.when(factory.createRequest(Mockito.any(), Mockito.any())).thenReturn(clientHttpRequest);
        final RestTemplate template = new RestTemplate(factory);
        final MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        final Counter counter = Mockito.mock(Counter.class);
        Mockito.when(registry.counter(Mockito.anyString())).thenReturn(counter);

        final JobRestController jobController = new JobRestController(
            Mockito.mock(JobCoordinatorService.class),
            this.jobSearchService,
            Mockito.mock(AttachmentService.class),
            Mockito.mock(ApplicationResourceAssembler.class),
            Mockito.mock(ClusterResourceAssembler.class),
            Mockito.mock(CommandResourceAssembler.class),
            Mockito.mock(JobResourceAssembler.class),
            Mockito.mock(JobRequestResourceAssembler.class),
            Mockito.mock(JobExecutionResourceAssembler.class),
            Mockito.mock(JobMetadataResourceAssembler.class),
            Mockito.mock(JobSearchResultResourceAssembler.class),
            new GenieHostInfo(this.hostname),
            template,
            this.genieResourceHttpRequestHandler,
            this.jobsProperties,
            registry
        );
        jobController.getJobOutput(jobId, forwardedFrom, request, response);

        Assert.assertThat(new String(bos.toByteArray(), UTF_8), Matchers.is(text));
        Mockito.verify(request, Mockito.times(1)).getHeader(HttpHeaders.ACCEPT);
        Mockito.verify(this.jobSearchService, Mockito.times(1)).getJobHost(Mockito.eq(jobId));
        Mockito.verify(response, Mockito.never()).sendError(Mockito.anyInt());
        Mockito.verify(this.genieResourceHttpRequestHandler, Mockito.never()).handleRequest(request, response);
    }
}
