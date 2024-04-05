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
package com.netflix.genie.web.apis.rest.v3.controllers;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.exceptions.GenieTimeoutException;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.common.internal.exceptions.checked.GenieConversionException;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.common.internal.exceptions.checked.JobArchiveException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieApplicationNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieIdAlreadyExistsException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.web.exceptions.checked.AttachmentTooLargeException;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.JobNotFoundException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.exceptions.checked.PreconditionFailedException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import com.netflix.genie.web.util.MetricsConstants;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.MethodArgumentNotValidException;

import javax.validation.ConstraintViolationException;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tests for the exception mapper.
 *
 * @author tgianos
 * @since 3.0.0
 */
class GenieExceptionMapperTest {

    private GenieExceptionMapper mapper;
    private MeterRegistry registry;
    private Counter counter;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.registry = Mockito.mock(MeterRegistry.class);
        this.counter = Mockito.mock(Counter.class);
        Mockito
            .when(
                this.registry.counter(
                    Mockito.eq(GenieExceptionMapper.CONTROLLER_EXCEPTION_COUNTER_NAME),
                    Mockito.anySet()
                )
            )
            .thenReturn(this.counter);
        this.mapper = new GenieExceptionMapper(this.registry);
    }

    /**
     * Test Genie Exceptions.
     */
    @Test
    void canHandleGenieExceptions() {
        final List<GenieException> exceptions = Arrays.asList(
            new GenieBadRequestException("bad"),
            new GenieNotFoundException("Not Found"),
            new GeniePreconditionException("Precondition"),
            new GenieServerException("server"),
            new GenieServerUnavailableException("Server Unavailable"),
            new GenieTimeoutException("Timeout"),
            new GenieException(568, "Other")
        );

        for (final GenieException exception : exceptions) {
            final ResponseEntity<GenieException> response = this.mapper.handleGenieException(exception);
            final HttpStatus expectedStatus = HttpStatus.resolve(exception.getErrorCode()) != null
                ? HttpStatus.resolve(exception.getErrorCode())
                : HttpStatus.INTERNAL_SERVER_ERROR;
            Assertions.assertThat(response.getStatusCode()).isEqualByComparingTo(expectedStatus);
            Mockito
                .verify(this.registry, Mockito.times(1))
                .counter(
                    GenieExceptionMapper.CONTROLLER_EXCEPTION_COUNTER_NAME,
                    Sets.newHashSet(
                        Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, exception.getClass().getCanonicalName())
                    )
                );
        }

        Mockito
            .verify(this.counter, Mockito.times(exceptions.size()))
            .increment();
    }

    /**
     * Test constraint violation exceptions.
     */
    @Test
    void canHandleConstraintViolationExceptions() {
        final ConstraintViolationException exception = new ConstraintViolationException("cve", null);
        final ResponseEntity<GeniePreconditionException> response = this.mapper.handleConstraintViolation(exception);
        Assertions.assertThat(response.getStatusCode()).isEqualByComparingTo(HttpStatus.PRECONDITION_FAILED);
        Mockito
            .verify(this.registry, Mockito.times(1))
            .counter(
                GenieExceptionMapper.CONTROLLER_EXCEPTION_COUNTER_NAME,
                Sets.newHashSet(
                    Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, exception.getClass().getCanonicalName())
                )
            );
        Mockito
            .verify(this.counter, Mockito.times(1))
            .increment();
    }

    /**
     * Test method argument not valid exceptions.
     */
    @Test
    @SuppressFBWarnings(value = "DM_NEW_FOR_GETCLASS", justification = "It's needed for the test")
    void canHandleMethodArgumentNotValidExceptions() {
        // Method is a final class so can't mock it. Just use the current method.
        final Method method = new Object() {
        }.getClass().getEnclosingMethod();
        final MethodParameter parameter = Mockito.mock(MethodParameter.class);
        Mockito.when(parameter.getMethod()).thenReturn(method);
        final Executable executable = Mockito.mock(Executable.class);
        Mockito.when(parameter.getExecutable()).thenReturn(executable);
        Mockito.when(executable.toGenericString()).thenReturn(UUID.randomUUID().toString());

        final BindingResult bindingResult = Mockito.mock(BindingResult.class);
        Mockito.when(bindingResult.getAllErrors()).thenReturn(Lists.newArrayList());

        final MethodArgumentNotValidException exception = new MethodArgumentNotValidException(
            parameter,
            bindingResult
        );

        final ResponseEntity<GeniePreconditionException> response =
            this.mapper.handleMethodArgumentNotValidException(exception);
        Assertions.assertThat(response.getStatusCode()).isEqualByComparingTo(HttpStatus.PRECONDITION_FAILED);
        Mockito
            .verify(this.registry, Mockito.times(1))
            .counter(
                GenieExceptionMapper.CONTROLLER_EXCEPTION_COUNTER_NAME,
                Sets.newHashSet(
                    Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, exception.getClass().getCanonicalName())
                )
            );
        Mockito
            .verify(this.counter, Mockito.times(1))
            .increment();
    }

    /**
     * Make sure the Runtime exceptions map to the expected error code.
     */
    @Test
    void canHandleGenieRuntimeExceptions() {
        final Map<GenieRuntimeException, HttpStatus> exceptions = Maps.newHashMap();

        exceptions.put(new GenieApplicationNotFoundException(), HttpStatus.NOT_FOUND);
        exceptions.put(new GenieClusterNotFoundException(), HttpStatus.NOT_FOUND);
        exceptions.put(new GenieCommandNotFoundException(), HttpStatus.NOT_FOUND);
        exceptions.put(new GenieJobNotFoundException(), HttpStatus.NOT_FOUND);
        exceptions.put(new GenieJobSpecificationNotFoundException(), HttpStatus.NOT_FOUND);
        exceptions.put(new GenieIdAlreadyExistsException(), HttpStatus.CONFLICT);
        exceptions.put(new GenieRuntimeException(), HttpStatus.INTERNAL_SERVER_ERROR);

        for (final Map.Entry<GenieRuntimeException, HttpStatus> exception : exceptions.entrySet()) {
            final ResponseEntity<GenieRuntimeException> response =
                this.mapper.handleGenieRuntimeException(exception.getKey());
            Assertions.assertThat(response.getStatusCode()).isEqualByComparingTo(exception.getValue());
        }
    }

    /**
     * Make sure the Runtime exceptions map to the expected error code.
     */
    @Test
    void canHandleGenieCheckedExceptions() {
        final Map<GenieCheckedException, HttpStatus> exceptions = Maps.newHashMap();

        exceptions.put(new GenieJobResolutionException(), HttpStatus.PRECONDITION_FAILED);
        exceptions.put(new JobArchiveException(), HttpStatus.INTERNAL_SERVER_ERROR);
        exceptions.put(new GenieConversionException(), HttpStatus.INTERNAL_SERVER_ERROR);
        exceptions.put(new GenieCheckedException(), HttpStatus.INTERNAL_SERVER_ERROR);
        exceptions.put(new IdAlreadyExistsException(), HttpStatus.CONFLICT);
        exceptions.put(new JobArchiveException(), HttpStatus.INTERNAL_SERVER_ERROR);
        exceptions.put(new SaveAttachmentException(), HttpStatus.INTERNAL_SERVER_ERROR);
        exceptions.put(new JobNotFoundException(), HttpStatus.NOT_FOUND);
        exceptions.put(new NotFoundException(), HttpStatus.NOT_FOUND);
        exceptions.put(new PreconditionFailedException(), HttpStatus.BAD_REQUEST);
        exceptions.put(new AttachmentTooLargeException(), HttpStatus.PAYLOAD_TOO_LARGE);

        for (final Map.Entry<GenieCheckedException, HttpStatus> exception : exceptions.entrySet()) {
            final ResponseEntity<GenieCheckedException> response =
                this.mapper.handleGenieCheckedException(exception.getKey());
            Assertions.assertThat(response.getStatusCode()).isEqualByComparingTo(exception.getValue());
        }
    }
}
