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

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotNull;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.Principal;

/**
 * Controller for forwarding UI requests.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Controller
public class UIController {

    /**
     * Return the getIndex.html template for requests to root.
     *
     * @param request  The request to get the principal from if there is one
     * @param response The servlet response to add cookies to
     * @return The index page
     */
    @GetMapping(
        value = {
            "/",
            "/applications/**",
            "/clusters/**",
            "/commands/**",
            "/jobs/**",
            "/output/**"
        }
    )
    public String getIndex(@NotNull final HttpServletRequest request, @NotNull final HttpServletResponse response) {
        final Principal principal = request.getUserPrincipal();
        if (principal != null) {
            response.addCookie(new Cookie("genie.user", principal.getName()));
        } else {
            response.addCookie(new Cookie("genie.user", "user@genie"));
        }
        return "index";
    }

    /**
     * Forward the file request to the API.
     *
     * @param id      The id of the job
     * @param request the servlet request to get path information from
     * @return The forward address to go to at the API endpoint
     * @throws UnsupportedEncodingException if URL-encoding of the job id fails
     */
    @GetMapping(value = "/file/{id}/**")
    public String getFile(
        @PathVariable("id") final String id,
        final HttpServletRequest request
    ) throws UnsupportedEncodingException {
        // When forwarding, the downstream ApplicationContext will perform URL decoding.
        // If the job ID contains special characters (such as '+'), they will be interpreted as url-encoded and
        // decoded, resulting in an invalid job ID that cannot be found.
        // To prevent this, always perform URL encoding on the ID before forwarding.
        final String encodedId = URLEncoder.encode(id, "UTF-8");
        final String path = "/api/v3/jobs/" + encodedId + "/" + ControllerUtils.getRemainingPath(request);
        return "forward:" + path;
    }
}
