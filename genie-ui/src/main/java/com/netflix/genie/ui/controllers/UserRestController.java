/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.ui.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.security.Principal;

/**
 * A helper {@link RestController} which allows the UI to request information about the current user.
 *
 * @author tgianos
 * @since 4.0.0
 */
@RestController
@RequestMapping(path = "/ui/user", produces = MediaType.APPLICATION_JSON_VALUE)
public class UserRestController {
    static final String NAME_KEY = "name";
    static final String DEFAULT_USER = "user@genie";

    /**
     * Get the information about the current session user.
     *
     * @param request The http request to get a principal from
     * @return The user information as a JSON object
     */
    @GetMapping
    public JsonNode getUserInfo(final HttpServletRequest request) {
        final Principal principal = request.getUserPrincipal();
        return JsonNodeFactory.instance
            .objectNode()
            .set(NAME_KEY, JsonNodeFactory.instance.textNode(principal == null ? DEFAULT_USER : principal.getName()));
    }
}
