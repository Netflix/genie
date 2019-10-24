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
package com.netflix.genie.web.apis.rest.v3.controllers;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.web.servlet.HandlerMapping;

import javax.servlet.http.HttpServletRequest;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;

/**
 * Unit tests for the ControllerUtils class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class ControllerUtilsTest {

    /**
     * Test the getRemainingPath method.
     */
    @Test
    void canGetRemainingPath() {
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE)).thenReturn(null);
        Assertions.assertThat(ControllerUtils.getRemainingPath(request)).isEqualTo("");

        Mockito
            .when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE))
            .thenReturn("/api/v3/jobs/1234/output/genie/log.out");
        Mockito
            .when(request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE))
            .thenReturn("/api/v3/jobs/{id}/output/**");
        Assertions.assertThat(ControllerUtils.getRemainingPath(request)).isEqualTo("genie/log.out");

        Mockito
            .when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE))
            .thenReturn("/api/v3/jobs/1234/output");
        Mockito
            .when(request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE))
            .thenReturn("/api/v3/jobs/{id}/output");
        Assertions.assertThat(ControllerUtils.getRemainingPath(request)).isEqualTo("");

        Mockito
            .when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE))
            .thenReturn("/api/v3/jobs/1234/output/");
        Mockito
            .when(request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE))
            .thenReturn("/api/v3/jobs/{id}/output/");
        Assertions.assertThat(ControllerUtils.getRemainingPath(request)).isEqualTo("");

        Mockito
            .when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE))
            .thenReturn("/api/v3/jobs/1234/output/stdout");
        Mockito
            .when(request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE))
            .thenReturn("/api/v3/jobs/{id}/output/**");
        Assertions.assertThat(ControllerUtils.getRemainingPath(request)).isEqualTo("stdout");
    }

    /**
     * Test {@link ControllerUtils#getRequestRoot(HttpServletRequest, String)}.
     *
     * @throws MalformedURLException shouldn't happen
     */
    @Test
    void canGetRequestRoot() throws MalformedURLException {
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final URL requestURL = new URL("https://genie.com/api/v3/jobs/1234/output/genie/genie.done?query=hi#DIV");
        final StringBuffer buffer = new StringBuffer(requestURL.toString());
        Mockito.when(request.getRequestURL()).thenReturn(buffer);

        Assertions
            .assertThat(ControllerUtils.getRequestRoot(request, ""))
            .isEqualTo(new URL("https://genie.com/api/v3/jobs/1234/output/genie/genie.done"));
        Assertions
            .assertThat(ControllerUtils.getRequestRoot(request, null))
            .isEqualTo(new URL("https://genie.com/api/v3/jobs/1234/output/genie/genie.done"));
        Assertions
            .assertThat(ControllerUtils.getRequestRoot(request, UUID.randomUUID().toString()))
            .isEqualTo(new URL("https://genie.com/api/v3/jobs/1234/output/genie/genie.done"));
        Assertions
            .assertThat(ControllerUtils.getRequestRoot(request, ".done"))
            .isEqualTo(new URL("https://genie.com/api/v3/jobs/1234/output/genie/genie"));
        Assertions
            .assertThat(ControllerUtils.getRequestRoot(request, "genie/genie.done"))
            .isEqualTo(new URL("https://genie.com/api/v3/jobs/1234/output/"));
    }
}
