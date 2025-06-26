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
package com.netflix.genie.web.apis.rest.v3.controllers;

import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import jakarta.servlet.http.HttpServletRequest;
/**
 * Custom error controller that properly handles 416 Range Not Satisfiable errors
 * which are incorrectly mapped to 500 errors by Spring Boot's default error handling.
 * (<a href="https://github.com/spring-projects/spring-framework/issues/34490">
 *     500 response for ResourceHttpRequestHandler when requested range is not satisfied</a>).
 * This class will not be needed after the code is upgraded to the next release of the spring
 * framework which will be released on July 17, 2025.
 */
@Controller
public class RangeErrorController implements ErrorController {

    private static final String ERROR_PATH = "/error";
    private static final String STATUS_CODE_ATTR = "jakarta.servlet.error.status_code";
    private static final String CONTENT_RANGE_HEADER = "Content-Range";
    private static final String BYTES_WILDCARD = "bytes */0";

    /**
     * Handles all error requests, with special handling for 416 errors.
     *
     * @param request the current request
     * @return an appropriate response entity or null to continue processing
     */
    @RequestMapping(ERROR_PATH)
    public ResponseEntity<Object> handleError(
        final HttpServletRequest request) {

        final Object statusCode = request.getAttribute(STATUS_CODE_ATTR);

        // Check if this is a 416 error
        if (statusCode instanceof Integer
            && (Integer) statusCode == HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE.value()) {

            // For 416 errors, return an empty body with proper headers
            return ResponseEntity
                .status(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                .header(CONTENT_RANGE_HEADER, BYTES_WILDCARD)
                .header(HttpHeaders.CONTENT_LENGTH, "0")
                .build();
        }

        // For other errors, let Spring continue with its default error handling
        // by returning null (which means no handler found, continue processing)
        return null;
    }
}
