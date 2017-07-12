/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.google.common.collect.Lists;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.exceptions.GenieTimeoutException;
import com.netflix.genie.core.util.MetricsConstants;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.MethodArgumentNotValidException;

import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;
import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Tests for the exception mapper.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieExceptionMapperUnitTests {

    private Counter badRequestRate;
    private Counter conflictRate;
    private Counter notFoundRate;
    private Counter preconditionRate;
    private Counter serverRate;
    private Counter serverUnavailableRate;
    private Counter timeoutRate;
    private Counter genieRate;
    private Counter constraintViolationRate;
    private Counter methodArgumentNotValidRate;

    private HttpServletResponse response;
    private GenieExceptionMapper mapper;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.badRequestRate = Mockito.mock(Counter.class);
        this.conflictRate = Mockito.mock(Counter.class);
        this.notFoundRate = Mockito.mock(Counter.class);
        this.preconditionRate = Mockito.mock(Counter.class);
        this.serverRate = Mockito.mock(Counter.class);
        this.serverUnavailableRate = Mockito.mock(Counter.class);
        this.timeoutRate = Mockito.mock(Counter.class);
        this.genieRate = Mockito.mock(Counter.class);
        this.constraintViolationRate = Mockito.mock(Counter.class);
        this.methodArgumentNotValidRate = Mockito.mock(Counter.class);

        final Registry registry = Mockito.mock(Registry.class);
        Mockito
            .when(registry.counter(MetricsConstants.GENIE_EXCEPTIONS_BAD_REQUEST_RATE))
            .thenReturn(this.badRequestRate);
        Mockito
            .when(registry.counter(MetricsConstants.GENIE_EXCEPTIONS_CONFLICT_RATE))
            .thenReturn(this.conflictRate);
        Mockito
            .when(registry.counter(MetricsConstants.GENIE_EXCEPTIONS_NOT_FOUND_RATE))
            .thenReturn(this.notFoundRate);
        Mockito
            .when(registry.counter(MetricsConstants.GENIE_EXCEPTIONS_PRECONDITION_RATE))
            .thenReturn(this.preconditionRate);
        Mockito
            .when(registry.counter(MetricsConstants.GENIE_EXCEPTIONS_SERVER_RATE))
            .thenReturn(this.serverRate);
        Mockito
            .when(registry.counter(MetricsConstants.GENIE_EXCEPTIONS_SERVER_UNAVAILABLE_RATE))
            .thenReturn(this.serverUnavailableRate);
        Mockito
            .when(registry.counter(MetricsConstants.GENIE_EXCEPTIONS_TIMEOUT_RATE))
            .thenReturn(this.timeoutRate);
        Mockito
            .when(registry.counter(MetricsConstants.GENIE_EXCEPTIONS_OTHER_RATE))
            .thenReturn(this.genieRate);
        Mockito
            .when(registry.counter(MetricsConstants.GENIE_EXCEPTIONS_CONSTRAINT_VIOLATION_RATE))
            .thenReturn(this.constraintViolationRate);
        Mockito
            .when(registry.counter(MetricsConstants.GENIE_EXCEPTIONS_METHOD_ARGUMENT_NOT_VALID_RATE))
            .thenReturn(this.methodArgumentNotValidRate);

        this.response = Mockito.mock(HttpServletResponse.class);
        this.mapper = new GenieExceptionMapper(registry);
    }

    /**
     * Test Genie Exceptions.
     *
     * @throws IOException on error
     */
    @Test
    public void canHandleGenieExceptions() throws IOException {
        this.mapper.handleGenieException(this.response, new GenieBadRequestException("bad"));
        this.mapper.handleGenieException(this.response, new GenieConflictException("conflict"));
        this.mapper.handleGenieException(this.response, new GenieNotFoundException("Not Found"));
        this.mapper.handleGenieException(this.response, new GeniePreconditionException("Precondition"));
        this.mapper.handleGenieException(this.response, new GenieServerException("server"));
        this.mapper.handleGenieException(this.response, new GenieServerUnavailableException("Server Unavailable"));
        this.mapper.handleGenieException(this.response, new GenieTimeoutException("Timeout"));
        this.mapper.handleGenieException(this.response, new GenieException(568, "Other"));

        Mockito.verify(this.badRequestRate, Mockito.times(1)).increment();
        Mockito.verify(this.conflictRate, Mockito.times(1)).increment();
        Mockito.verify(this.notFoundRate, Mockito.times(1)).increment();
        Mockito.verify(this.preconditionRate, Mockito.times(1)).increment();
        Mockito.verify(this.serverRate, Mockito.times(1)).increment();
        Mockito.verify(this.serverUnavailableRate, Mockito.times(1)).increment();
        Mockito.verify(this.timeoutRate, Mockito.times(1)).increment();
        Mockito.verify(this.genieRate, Mockito.times(1)).increment();
        Mockito.verify(this.response, Mockito.times(8)).sendError(Mockito.anyInt(), Mockito.anyString());
    }

    /**
     * Test constraint violation exceptions.
     *
     * @throws IOException on error
     */
    @Test
    public void canHandleConstraintViolationExceptions() throws IOException {
        this.mapper.handleConstraintViolation(this.response, new ConstraintViolationException("cve", null));
        Mockito.verify(this.constraintViolationRate, Mockito.times(1)).increment();
        Mockito.verify(this.response, Mockito.times(1))
            .sendError(Mockito.eq(HttpStatus.PRECONDITION_FAILED.value()), Mockito.anyString());
    }

    /**
     * Test method argument not valid exceptions.
     *
     * @throws IOException on error
     */
    @Test
    @SuppressFBWarnings(value = "DM_NEW_FOR_GETCLASS", justification = "It's needed for the test")
    public void canHandleMethodArgumentNotValidExceptions() throws IOException {
        // Method is a final class so can't mock it. Just use the current method.
        final Method method = new Object() {
        }.getClass().getEnclosingMethod();
        final MethodParameter parameter = Mockito.mock(MethodParameter.class);
        Mockito.when(parameter.getMethod()).thenReturn(method);

        final BindingResult bindingResult = Mockito.mock(BindingResult.class);
        Mockito.when(bindingResult.getAllErrors()).thenReturn(Lists.newArrayList());

        this.mapper.handleMethodArgumentNotValidException(
            this.response,
            new MethodArgumentNotValidException(
                parameter,
                bindingResult
            )
        );
        Mockito
            .verify(this.methodArgumentNotValidRate, Mockito.times(1))
            .increment();
        Mockito
            .verify(this.response, Mockito.times(1))
            .sendError(Mockito.eq(HttpStatus.PRECONDITION_FAILED.value()), Mockito.anyString());
    }
}
