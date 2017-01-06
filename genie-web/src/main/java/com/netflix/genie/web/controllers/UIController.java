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

import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.annotation.Nullable;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotNull;

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
     * @param response       The servlet response to add cookies to
     * @param authentication The Spring Security authentication if present
     * @return getIndex
     */
    @RequestMapping(
        value = {
            "/",
            "/applications/**",
            "/clusters/**",
            "/commands/**",
            "/jobs/**",
            "/output/**"
        },
        method = RequestMethod.GET
    )
    public String getIndex(@NotNull final HttpServletResponse response, @Nullable final Authentication authentication) {
        if (authentication != null) {
            response.addCookie(new Cookie("genie.user", authentication.getName()));
        } else {
            response.addCookie(new Cookie("genie.user", "user@genie"));
        }
        return "index";
    }

    /**
     * Forward the file request to the API.
     *
     * @param id The id of the job
     * @param request the servlet request to get path information from
     * @return The forward address to go to at the API endpoint
     */
    @RequestMapping(value = "/file/{id}/**", method = RequestMethod.GET)
    public String getFile(@PathVariable("id") final String id, final HttpServletRequest request) {
        return "forward:/api/v3/jobs/" + id + "/" + ControllerUtils.getRemainingPath(request);
    }
}
