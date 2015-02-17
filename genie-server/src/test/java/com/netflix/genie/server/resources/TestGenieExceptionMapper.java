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
package com.netflix.genie.server.resources;

import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;

/**
 * Tests for the exception mapper.
 *
 * @author tgianos
 */
public class TestGenieExceptionMapper {
    private GenieExceptionMapper mapper;
    private static final String ERROR_MESSAGE = "Genie error";

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.mapper = new GenieExceptionMapper();
    }

    /**
     * Test 400.
     */
    @Test
    public void testGenieBadRequestException() {
        final GenieException ge = new GenieBadRequestException(ERROR_MESSAGE);
        final Response response = this.mapper.toResponse(ge);
        Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getStatus());
        Assert.assertNotNull(response.getEntity());
        Assert.assertEquals(ERROR_MESSAGE, response.getEntity());
    }

    /**
     * Test 409.
     */
    @Test
    public void testGenieConflictException() {
        final GenieException ge = new GenieConflictException(ERROR_MESSAGE);
        final Response response = this.mapper.toResponse(ge);
        Assert.assertEquals(HttpURLConnection.HTTP_CONFLICT, response.getStatus());
        Assert.assertNotNull(response.getEntity());
        Assert.assertEquals(ERROR_MESSAGE, response.getEntity());
    }

    /**
     * Test random exception.
     */
    @Test
    public void testGenieException() {
        final GenieException ge = new GenieException(300, ERROR_MESSAGE);
        final Response response = this.mapper.toResponse(ge);
        Assert.assertEquals(300, response.getStatus());
        Assert.assertNotNull(response.getEntity());
        Assert.assertEquals(ERROR_MESSAGE, response.getEntity());
    }

    /**
     * Test 404.
     */
    @Test
    public void testGenieNotFoundException() {
        final GenieException ge = new GenieNotFoundException(ERROR_MESSAGE);
        final Response response = this.mapper.toResponse(ge);
        Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.getStatus());
        Assert.assertNotNull(response.getEntity());
        Assert.assertEquals(ERROR_MESSAGE, response.getEntity());
    }

    /**
     * Test 412.
     */
    @Test
    public void testGeniePreconditionException() {
        final GenieException ge = new GeniePreconditionException(ERROR_MESSAGE);
        final Response response = this.mapper.toResponse(ge);
        Assert.assertEquals(HttpURLConnection.HTTP_PRECON_FAILED, response.getStatus());
        Assert.assertNotNull(response.getEntity());
        Assert.assertEquals(ERROR_MESSAGE, response.getEntity());
    }

    /**
     * Test 500.
     */
    @Test
    public void testGenieServerException() {
        final GenieException ge = new GenieServerException(ERROR_MESSAGE);
        final Response response = this.mapper.toResponse(ge);
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.getStatus());
        Assert.assertNotNull(response.getEntity());
        Assert.assertEquals(ERROR_MESSAGE, response.getEntity());
    }
}
