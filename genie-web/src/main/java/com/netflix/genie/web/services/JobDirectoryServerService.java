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
package com.netflix.genie.web.services;

import org.springframework.validation.annotation.Validated;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;

/**
 * This service abstracts away the details of responding to API requests for the files and directories created during
 * the execution of a job in the Genie ecosystem. This service is read only.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface JobDirectoryServerService {

    /**
     * Given the {@code request} this API will write the resource to {@code response} if possible. If the resource
     * doesn't exist or an error is generated an appropriate HTTP error response will be written to {@code response}
     * instead.
     *
     * @param jobId        The id of the job this request is for
     * @param baseUrl      The base URL used to generate all URLs for resources
     * @param relativePath The relative path from the root of the job directory of the expected resource
     * @param request      The HTTP request containing all information about the request
     * @param response     The HTTP response where all results should be written
     * @throws IOException      If there is an error interacting with the response
     * @throws ServletException If there is an error interacting with the Java Servlet objects
     */
    void serveResource(
        String jobId,
        URL baseUrl,
        String relativePath,
        HttpServletRequest request,
        HttpServletResponse response
    ) throws IOException, ServletException;
}
