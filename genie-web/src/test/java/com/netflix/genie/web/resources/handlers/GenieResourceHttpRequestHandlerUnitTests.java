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
package com.netflix.genie.web.resources.handlers;

import com.google.common.collect.Lists;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.resources.writers.DirectoryWriter;
import com.netflix.genie.web.services.JobFileService;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.HandlerMapping;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

/**
 * Unit tests for the GenieResourceHttpRequestHandler class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieResourceHttpRequestHandlerUnitTests {

    private DirectoryWriter directoryWriter;
    private GenieResourceHttpRequestHandler handler;
    private JobFileService jobFileService;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.directoryWriter = Mockito.mock(DirectoryWriter.class);
        this.jobFileService = Mockito.mock(JobFileService.class);
        this.handler = new GenieResourceHttpRequestHandler(this.directoryWriter, this.jobFileService);
    }

    /**
     * Make sure we can't handle any requests for resources without a resource location to look in.
     *
     * @throws ServletException on error
     * @throws IOException      on error
     */
    @Test(expected = IllegalStateException.class)
    public void cantHandleRequestWithOutAnyLocations() throws ServletException, IOException {
        this.handler.setLocations(Lists.newArrayList());
        this.handler.handleRequest(Mockito.mock(HttpServletRequest.class), Mockito.mock(HttpServletResponse.class));
    }

    /**
     * Make sure we can't handle any requests for resources with more than one resource available.
     *
     * @throws ServletException on error
     * @throws IOException      on error
     */
    @Test(expected = IllegalStateException.class)
    public void cantHandleRequestWithTooManyLocations() throws ServletException, IOException {
        this.handler.setLocations(Lists.newArrayList(Mockito.mock(Resource.class), Mockito.mock(Resource.class)));
        this.handler.handleRequest(Mockito.mock(HttpServletRequest.class), Mockito.mock(HttpServletResponse.class));
    }

    /**
     * Make sure we can't handle the HTTP request for a resource if we don't have the proper attribute in the request.
     *
     * @throws ServletException on error
     * @throws IOException      on error
     */
    @Test(expected = IllegalStateException.class)
    public void cantHandleRequestWithoutHandlerMappingAttribute() throws ServletException, IOException {
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE)).thenReturn(null);
        this.handler.handleRequest(request, response);
    }

    /**
     * Make sure if the resource doesn't exist we return a 404.
     *
     * @throws ServletException On any error
     * @throws IOException      On any error
     */
    @Test
    public void cantHandleRequestIfResourceDoesntExist() throws ServletException, IOException {
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        final String jobId = UUID.randomUUID().toString();
        final String path = UUID.randomUUID().toString();
        Mockito.when(request.getAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_ID_ATTRIBUTE)).thenReturn(jobId);
        Mockito.when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE)).thenReturn(path);
        final Resource resource = Mockito.mock(Resource.class);
        Mockito.when(this.jobFileService.getJobFileAsResource(jobId, path)).thenReturn(resource);
        Mockito.when(resource.exists()).thenReturn(false);

        this.handler.handleRequest(request, response);

        Mockito.verify(response, Mockito.times(1)).sendError(HttpStatus.NOT_FOUND.value());
    }

    /**
     * Make sure if the resource isn't a directory it's sent to super.
     * <p>
     * Note: This doesn't actually test returning a file as we leverage Spring's implementation
     * which we assume is working.
     *
     * @throws ServletException On any error
     * @throws IOException      On any error
     */
    @Test
    public void canHandleRequestForFile() throws ServletException, IOException {
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        final String jobId = UUID.randomUUID().toString();
        final String path = UUID.randomUUID().toString();
        Mockito.when(request.getAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_ID_ATTRIBUTE)).thenReturn(jobId);
        Mockito.when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE)).thenReturn(path);
        final Resource resource = Mockito.mock(Resource.class);
        // The second time will return the 404
        Mockito.when(this.jobFileService.getJobFileAsResource(jobId, path)).thenReturn(resource).thenReturn(null);
        Mockito.when(resource.exists()).thenReturn(true);
        final File file = Mockito.mock(File.class);
        Mockito.when(resource.getFile()).thenReturn(file);
        Mockito.when(file.isDirectory()).thenReturn(false);

        this.handler.handleRequest(request, response);

        Mockito.verify(response, Mockito.times(1)).sendError(HttpStatus.NOT_FOUND.value());
    }

    /**
     * Make sure if the resource is a directory as HTML it's handled properly.
     *
     * @throws Exception On any error
     */
    @Test
    public void canHandleRequestForDirectoryHtml() throws Exception {
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        final String jobId = UUID.randomUUID().toString();
        final String path = UUID.randomUUID().toString();
        Mockito.when(request.getAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_ID_ATTRIBUTE)).thenReturn(jobId);
        Mockito.when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE)).thenReturn(path);
        Mockito.when(request.getHeader(HttpHeaders.ACCEPT)).thenReturn(MediaType.TEXT_HTML_VALUE);
        final String forwardedUrl = UUID.randomUUID().toString();
        Mockito.when(request.getHeader(JobConstants.GENIE_FORWARDED_FROM_HEADER)).thenReturn(forwardedUrl);
        final Resource resource = Mockito.mock(Resource.class);
        Mockito.when(this.jobFileService.getJobFileAsResource(jobId, path)).thenReturn(resource);
        Mockito.when(resource.exists()).thenReturn(true);
        final File file = Mockito.mock(File.class);
        Mockito.when(resource.getFile()).thenReturn(file);
        Mockito.when(file.isDirectory()).thenReturn(true);

        final String html = UUID.randomUUID().toString();

        Mockito.when(
            this.directoryWriter.toHtml(Mockito.eq(file), Mockito.eq(forwardedUrl), Mockito.eq(false))
        ).thenReturn(html);

        final ServletOutputStream os = Mockito.mock(ServletOutputStream.class);
        Mockito.when(response.getOutputStream()).thenReturn(os);

        this.handler.handleRequest(request, response);

        Mockito.verify(response, Mockito.times(1)).setContentType(MediaType.TEXT_HTML_VALUE);
        Mockito.verify(response, Mockito.times(1)).getOutputStream();
        Mockito.verify(this.directoryWriter, Mockito.times(1))
            .toHtml(Mockito.eq(file), Mockito.eq(forwardedUrl), Mockito.eq(false));
        Mockito.verify(os, Mockito.times(1)).write(html.getBytes(Charset.forName("UTF-8")));
    }

    /**
     * Make sure if the resource is a directory as JSON it's handled properly.
     *
     * @throws Exception On any error
     */
    @Test
    public void canHandleRequestForDirectoryJson() throws Exception {
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        final String jobId = UUID.randomUUID().toString();
        final String path = UUID.randomUUID().toString();
        Mockito.when(request.getAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_ID_ATTRIBUTE)).thenReturn(jobId);
        Mockito.when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE)).thenReturn(path);
        Mockito.when(request.getHeader(HttpHeaders.ACCEPT)).thenReturn(null);
        final String requestUrl = UUID.randomUUID().toString();
        Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer(requestUrl));
        final Resource resource = Mockito.mock(Resource.class);
        Mockito.when(this.jobFileService.getJobFileAsResource(jobId, path)).thenReturn(resource);
        Mockito.when(resource.exists()).thenReturn(true);
        final File file = Mockito.mock(File.class);
        Mockito.when(resource.getFile()).thenReturn(file);
        Mockito.when(file.isDirectory()).thenReturn(true);

        final String html = UUID.randomUUID().toString();

        Mockito.when(
            this.directoryWriter.toJson(Mockito.eq(file), Mockito.eq(requestUrl), Mockito.eq(false))
        ).thenReturn(html);

        final ServletOutputStream os = Mockito.mock(ServletOutputStream.class);
        Mockito.when(response.getOutputStream()).thenReturn(os);

        this.handler.handleRequest(request, response);

        Mockito.verify(response, Mockito.times(1)).setContentType(MediaType.APPLICATION_JSON_VALUE);
        Mockito.verify(response, Mockito.times(1)).getOutputStream();
        Mockito.verify(this.directoryWriter, Mockito.times(1))
            .toJson(Mockito.eq(file), Mockito.eq(requestUrl), Mockito.eq(false));
        Mockito.verify(os, Mockito.times(1)).write(html.getBytes(Charset.forName("UTF-8")));
    }

    /**
     * Make sure if the resource is a directory it's handled properly until exception thrown.
     *
     * @throws Exception On any error
     */
    @Test(expected = ServletException.class)
    public void cantHandleRequestForDirectoryWhenException() throws Exception {
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        final String jobId = UUID.randomUUID().toString();
        final String path = UUID.randomUUID().toString();
        Mockito.when(request.getAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_ID_ATTRIBUTE)).thenReturn(jobId);
        Mockito.when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE)).thenReturn(path);
        Mockito
            .when(request.getAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_IS_ROOT_DIRECTORY))
            .thenReturn(false);
        final String requestUrl = UUID.randomUUID().toString();
        Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer(requestUrl));
        final Resource resource = Mockito.mock(Resource.class);
        Mockito.when(this.jobFileService.getJobFileAsResource(jobId, path)).thenReturn(resource);
        Mockito.when(resource.exists()).thenReturn(true);
        final File file = Mockito.mock(File.class);
        Mockito.when(resource.getFile()).thenReturn(file);
        Mockito.when(file.isDirectory()).thenReturn(true);

        Mockito.when(
            this.directoryWriter.toHtml(Mockito.eq(file), Mockito.eq(requestUrl), Mockito.eq(true))
        ).thenThrow(new Exception());

        final ServletOutputStream os = Mockito.mock(ServletOutputStream.class);
        Mockito.when(response.getOutputStream()).thenReturn(os);

        this.handler.handleRequest(request, response);
    }

    /**
     * Make sure we can use the overridden set headers method properly for large file sizes.
     *
     * @throws IOException on error
     */
    // TODO: Now using the default implementation of this so can't access anymore may want to make sure these tests
    //       Are covered in above testing
    @Test
    @Ignore
    public void canSetHeaders() throws IOException {
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        final Resource resource = Mockito.mock(Resource.class);
//        final MediaType mediaType = MediaType.APPLICATION_OCTET_STREAM;

        final long justRight = (long) Integer.MAX_VALUE;
        final long tooLong = (long) Integer.MAX_VALUE + 1;
        Mockito
            .when(resource.contentLength())
            .thenReturn(justRight)
            .thenReturn(tooLong);

//        this.handler.setHeaders(response, resource, mediaType);
//        this.handler.setHeaders(response, resource, null);

        Mockito.verify(response, Mockito.times(1)).setContentLengthLong(justRight);
        Mockito.verify(response, Mockito.times(1)).setContentLengthLong(tooLong);
        Mockito.verify(response, Mockito.times(1)).setContentType(Mockito.anyString());
    }
}
