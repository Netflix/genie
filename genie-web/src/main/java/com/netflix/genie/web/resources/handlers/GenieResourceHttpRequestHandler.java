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

import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.web.resources.writers.DirectoryWriter;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.util.Assert;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Class extends ResourceHttpRequestHandler to override handling a request to return directory listing if it
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

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private DirectoryWriter directoryWriter;

    /**
     * Constructor.
     *
     * @param directoryWriter The class to use to convert directories to representations like HTML
     */
    public GenieResourceHttpRequestHandler(final DirectoryWriter directoryWriter) {
        super();
        this.directoryWriter = directoryWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleRequest(
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws ServletException, IOException {
        Assert.state(this.getLocations().size() == 1, "Too many resource locations");
        Assert.state(
            request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE) != null,
            "Request doesn't have a " + HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE + " attribute."
        );

        final String path = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
        final Resource resource = this.getLocations().get(0).createRelative(path);
        if (!resource.exists()) {
            response.sendError(HttpStatus.NOT_FOUND.value());
            return;
        }

        final File file = resource.getFile();
        if (file.isDirectory()) {
            final Object rootDirAttribute = request.getAttribute(GENIE_JOB_IS_ROOT_DIRECTORY);
            final boolean isRootDirectory = rootDirAttribute != null ? (Boolean) rootDirAttribute : true;
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
}
