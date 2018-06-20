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

import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.resources.writers.DirectoryWriter;
import com.netflix.genie.web.services.JobFileService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;

import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Class extends {@link ResourceHttpRequestHandler} to override handling a request to return directory listing if it
 * is a directory otherwise follow default behavior.
 *
 * @author tgianos
 * @see ResourceHttpRequestHandler
 * @since 3.0.0
 */
public class GenieResourceHttpRequestHandler extends ResourceHttpRequestHandler {

    /**
     * Used to flag if this is the root directory or not for a given job. If not present defaults to true.
     */
    public static final String GENIE_JOB_IS_ROOT_DIRECTORY
        = GenieResourceHttpRequestHandler.class.getName() + ".isRootDirectory";

    /**
     * Used to identify the id of the job that is being requested.
     */
    public static final String GENIE_JOB_ID_ATTRIBUTE = GenieResourceHttpRequestHandler.class.getName() + ".jobId";

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private final DirectoryWriter directoryWriter;
    private final JobFileService jobFileService;

    /**
     * Constructor.
     *
     * @param directoryWriter The class to use to convert directories to representations like HTML
     * @param jobFileService  The log service to use
     */
    public GenieResourceHttpRequestHandler(final DirectoryWriter directoryWriter, final JobFileService jobFileService) {
        super();
        this.directoryWriter = directoryWriter;
        this.jobFileService = jobFileService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleRequest(
        @Nonnull final HttpServletRequest request,
        @Nonnull final HttpServletResponse response
    ) throws ServletException, IOException {
        final Resource resource = this.getResource(request);
        if (resource == null || !resource.exists()) {
            response.sendError(HttpStatus.NOT_FOUND.value());
            return;
        }

        final File file = resource.getFile();
        if (file.isDirectory()) {
            final Object rootDirAttribute = request.getAttribute(GENIE_JOB_IS_ROOT_DIRECTORY);
            final boolean isRootDirectory;
            if (rootDirAttribute != null) {
                isRootDirectory = (Boolean) rootDirAttribute;
            } else {
                isRootDirectory = true;
            }
            final String accept = request.getHeader(HttpHeaders.ACCEPT);
            final String requestUrl;
            if (request.getHeader(JobConstants.GENIE_FORWARDED_FROM_HEADER) != null) {
                requestUrl = request.getHeader(JobConstants.GENIE_FORWARDED_FROM_HEADER);
            } else {
                requestUrl = request.getRequestURL().toString();
            }

            try {
                if (accept != null && accept.contains(MediaType.TEXT_HTML_VALUE)) {
                    response.setContentType(MediaType.TEXT_HTML_VALUE);
                    response
                        .getOutputStream()
                        .write(
                            this.directoryWriter.toHtml(
                                file,
                                requestUrl,
                                !isRootDirectory
                            ).getBytes(UTF_8)
                        );
                } else {
                    response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                    response
                        .getOutputStream()
                        .write(
                            this.directoryWriter.toJson(
                                file,
                                requestUrl,
                                !isRootDirectory
                            ).getBytes(UTF_8)
                        );
                }
            } catch (final Exception e) {
                throw new ServletException(e);
            }
        } else {
            super.handleRequest(request, response);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Default to using the {@link JobFileService} implementation.
     */
    @Override
    protected Resource getResource(final HttpServletRequest request) throws IOException {
        final String jobId = (String) request.getAttribute(GENIE_JOB_ID_ATTRIBUTE);
        if (StringUtils.isBlank(jobId)) {
            throw new IllegalStateException("Required request attribute '" + GENIE_JOB_ID_ATTRIBUTE + "' is not set");
        }
        final String relativePath = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);

        return this.jobFileService.getJobFileAsResource(jobId, relativePath);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overriding to handle case where media type was unknown to default to Text
     */
    @Override
    protected MediaType getMediaType(final HttpServletRequest request, @Nonnull final Resource resource) {
        final MediaType mediaType = super.getMediaType(request, resource);
        return mediaType == null ? MediaType.TEXT_PLAIN : mediaType;
    }
}
