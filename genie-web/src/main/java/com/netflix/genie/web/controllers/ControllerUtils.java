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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.servlet.HandlerMapping;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

/**
 * Utility methods re-used in various controllers.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public final class ControllerUtils {

    private static final String EMPTY_STRING = "";

    /**
     * Constructor.
     */
    private ControllerUtils() {
    }

    /**
     * Get the remaining path from a given request. e.g. if the request went to a method with the matching pattern of
     * /api/v3/jobs/{id}/output/** and the request was /api/v3/jobs/{id}/output/blah.txt the return value of this
     * method would be blah.txt.
     *
     * @param request The http servlet request.
     * @return The remaining path
     */
    public static String getRemainingPath(final HttpServletRequest request) {
        String path = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
        if (path != null) {
            final String bestMatchPattern
                = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
            log.debug("bestMatchPattern = {}", bestMatchPattern);
            path = new AntPathMatcher().extractPathWithinPattern(bestMatchPattern, path);
        }
        path = path == null ? EMPTY_STRING : path;
        log.debug("Remaining path = {}", path);
        return path;
    }

    /**
     * Given a HTTP {@code request} and a {@code path} this method will return the root of the request minus the path.
     * Generally the path will be derived from {@link #getRemainingPath(HttpServletRequest)} and this method will be
     * called subsequently.
     * <p>
     * If the request URL is {@code https://myhost/api/v3/jobs/12345/output/genie/run} and the path is {@code genie/run}
     * this method should return {@code https://myhost/api/v3/jobs/12345/output/}.
     *
     * @param request The HTTP request to get information from
     * @param path    The path that should be removed from the end of the request URL
     * @return The base of the request
     * @since 4.0.0
     */
    static String getRequestRoot(final HttpServletRequest request, @Nullable final String path) {
        return getRequestRoot(request.getRequestURL().toString(), path);
    }

    /**
     * Given a HTTP {@code request} and a {@code path} this method will return the root of the request minus the path.
     * Generally the path will be derived from {@link #getRemainingPath(HttpServletRequest)} and this method will be
     * called subsequently.
     * <p>
     * If the request URL is {@code https://myhost/api/v3/jobs/12345/output/genie/run} and the path is {@code genie/run}
     * this method should return {@code https://myhost/api/v3/jobs/12345/output/}.
     *
     * @param request The HTTP request to get information from
     * @param path    The path that should be removed from the end of the request URL
     * @return The base of the request
     * @since 4.0.0
     */
    static String getRequestRoot(final String request, @Nullable final String path) {
        return StringUtils.removeEnd(request, path);
    }
}
