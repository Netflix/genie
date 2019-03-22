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
package com.netflix.genie.common.exceptions;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * Test the constructors of the GenieTimeoutException.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class GenieTimeoutExceptionTest extends Exception {

    private static final String ERROR_MESSAGE = "Not Found";
    private static final IOException IOE = new IOException("IOException");

    /**
     * Test the constructor.
     *
     * @throws GenieTimeoutException On an issue
     */
    @Test(expected = GenieTimeoutException.class)
    public void testTwoArgConstructor() throws GenieTimeoutException {
        final GenieTimeoutException ge = new GenieTimeoutException(ERROR_MESSAGE, IOE);
        Assert.assertEquals(HttpURLConnection.HTTP_CLIENT_TIMEOUT, ge.getErrorCode());
        Assert.assertEquals(ERROR_MESSAGE, ge.getMessage());
        Assert.assertEquals(IOE, ge.getCause());
        throw ge;
    }

    /**
     * Test the constructor.
     *
     * @throws GenieTimeoutException On an issue
     */
    @Test(expected = GenieTimeoutException.class)
    public void testMessageArgConstructor() throws GenieTimeoutException {
        final GenieTimeoutException ge = new GenieTimeoutException(ERROR_MESSAGE);
        Assert.assertEquals(HttpURLConnection.HTTP_CLIENT_TIMEOUT, ge.getErrorCode());
        Assert.assertEquals(ERROR_MESSAGE, ge.getMessage());
        Assert.assertNull(ge.getCause());
        throw ge;
    }
}
