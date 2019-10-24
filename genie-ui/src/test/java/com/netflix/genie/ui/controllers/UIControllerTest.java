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
package com.netflix.genie.ui.controllers;

import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.web.servlet.HandlerMapping;

import javax.servlet.http.HttpServletRequest;
import java.net.URLEncoder;
import java.util.UUID;

/**
 * Unit tests for UIController class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class UIControllerTest {

    private UIController controller;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.controller = new UIController();
    }

    /**
     * Make sure the getIndex endpoint returns the right template name.
     */
    @Test
    void canGetIndex() {
        Assert.assertThat(this.controller.getIndex(), Matchers.is("index"));
    }

    /**
     * Make sure the getFile method returns the right forward command.
     *
     * @throws Exception if an error occurs
     */
    @Test
    void canGetFile() throws Exception {
        final String id = UUID.randomUUID().toString();
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito
            .when(request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE))
            .thenReturn("/file/" + id + "/output/genie/log.out");
        Mockito
            .when(request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE))
            .thenReturn("/file/{id}/**");

        final String encodedId = URLEncoder.encode(id, "UTF-8");
        final String expectedPath = "/api/v3/jobs/" + encodedId + "/output/genie/log.out";

        Assertions.assertThat(this.controller.getFile(id, request)).isEqualTo("forward:" + expectedPath);
    }
}
