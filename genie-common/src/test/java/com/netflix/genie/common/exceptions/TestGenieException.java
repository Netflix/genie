/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test the constructors of the GenieException.
 *
 * @author tgianos
 */
public class TestGenieException {
    private static final int ERROR_CODE = 404;
    private static final String ERROR_MESSAGE = "Not Found";
    private static final IOException IOE = new IOException("IOException");

    /**
     * Test the constructor.
     */
    @Test
    public void testThreeArgConstructor() {
        final GenieException ge = new GenieException(ERROR_CODE, ERROR_MESSAGE, IOE);
        Assert.assertEquals(ERROR_CODE, ge.getErrorCode());
        Assert.assertEquals(ERROR_MESSAGE, ge.getMessage());
        Assert.assertEquals(IOE, ge.getCause());
    }

    /**
     * Test the constructor.
     */
    @Test
    public void testTwoArgConstructorWithMessage() {
        final GenieException ge = new GenieException(ERROR_CODE, ERROR_MESSAGE);
        Assert.assertEquals(ERROR_CODE, ge.getErrorCode());
        Assert.assertEquals(ERROR_MESSAGE, ge.getMessage());
        Assert.assertNull(ge.getCause());
    }

    /**
     * Test the constructor.
     */
    @Test
    public void testTwoArgConstructorWithThrowable() {
        final GenieException ge = new GenieException(ERROR_CODE, IOE);
        Assert.assertEquals(ERROR_CODE, ge.getErrorCode());
        Assert.assertEquals(IOE, ge.getCause());
    }
}
