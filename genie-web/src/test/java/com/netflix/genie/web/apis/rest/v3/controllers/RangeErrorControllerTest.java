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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import jakarta.servlet.RequestDispatcher;
import org.assertj.core.api.Assertions;

/**
 * Unit tests for {@link RangeErrorController}.
 */
class RangeErrorControllerTest {

    private RangeErrorController controller;
    private MockHttpServletRequest request;

    @BeforeEach
    void setUp() {
        this.controller = new RangeErrorController();
        this.request = new MockHttpServletRequest();
    }

    /**
     * Test that the controller correctly handles 416 errors.
     */
    @Test
    void testHandle416Error() {
        // Set up request with 416 status code
        this.request.setAttribute(RequestDispatcher.ERROR_STATUS_CODE, HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE.value());

        // Call the controller
        final ResponseEntity<Object> responseEntity = this.controller.handleError(this.request);

        // Verify response
        Assertions.assertThat(responseEntity).isNotNull();
        Assertions.assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
        Assertions.assertThat(responseEntity.getHeaders().getFirst("Content-Range")).isEqualTo("bytes */0");
        Assertions.assertThat(responseEntity.getHeaders().getFirst(HttpHeaders.CONTENT_LENGTH)).isEqualTo("0");
        Assertions.assertThat(responseEntity.getBody()).isNull();
    }

    /**
     * Test that the controller returns null for non-416 errors.
     */
    @Test
    void testHandleOtherErrors() {
        // Set up request with 404 status code
        this.request.setAttribute(RequestDispatcher.ERROR_STATUS_CODE, HttpStatus.NOT_FOUND.value());

        // Call the controller
        final ResponseEntity<Object> responseEntity = this.controller.handleError(this.request);

        // Verify response is null (letting Spring handle it)
        Assertions.assertThat(responseEntity).isNull();
    }

    /**
     * Test that the controller returns null when status code is missing.
     */
    @Test
    void testHandleMissingStatusCode() {
        // No status code set in request

        // Call the controller
        final ResponseEntity<Object> responseEntity = this.controller.handleError(this.request);

        // Verify response is null (letting Spring handle it)
        Assertions.assertThat(responseEntity).isNull();
    }

    /**
     * Test that the controller returns null when status code is not an Integer.
     */
    @Test
    void testHandleNonIntegerStatusCode() {
        // Set up request with non-integer status code
        this.request.setAttribute(RequestDispatcher.ERROR_STATUS_CODE, "416");

        // Call the controller
        final ResponseEntity<Object> responseEntity = this.controller.handleError(this.request);

        // Verify response is null (letting Spring handle it)
        Assertions.assertThat(responseEntity).isNull();
    }
}
