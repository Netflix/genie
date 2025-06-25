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
import org.springframework.mock.web.MockHttpServletResponse;

import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        assertThat(responseEntity).isNotNull();
        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
        assertThat(responseEntity.getHeaders().getFirst("Content-Range")).isEqualTo("bytes */0");
        assertThat(responseEntity.getHeaders().getFirst(HttpHeaders.CONTENT_LENGTH)).isEqualTo("0");
        assertThat(responseEntity.getBody()).isNull();
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
        assertThat(responseEntity).isNull();
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
        assertThat(responseEntity).isNull();
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
        assertThat(responseEntity).isNull();
    }

    /**
     * Test with mocked request to verify attribute access.
     */
    @Test
    void testWithMockedRequest() {
        // Create mock request
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getAttribute(RequestDispatcher.ERROR_STATUS_CODE))
            .thenReturn(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE.value());

        // Call the controller
        final ResponseEntity<Object> responseEntity = this.controller.handleError(mockRequest);

        // Verify response
        assertThat(responseEntity).isNotNull();
        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
    }
}
