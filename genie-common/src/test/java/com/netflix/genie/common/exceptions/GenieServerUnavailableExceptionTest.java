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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * Test the constructors of the {@link GenieServerUnavailableException}.
 *
 * @author tgianos
 * @since 2.0.0
 */
class GenieServerUnavailableExceptionTest {

    private static final String ERROR_MESSAGE = "Cannot Run Jobs";
    private static final IOException IOE = new IOException("IOException");

    /**
     * Test the constructor.
     */
    @Test
    void testTwoArgConstructor() {
        Assertions
            .assertThatExceptionOfType(GenieServerUnavailableException.class)
            .isThrownBy(
                () -> {
                    throw new GenieServerUnavailableException(ERROR_MESSAGE, IOE);
                }
            )
            .withCause(IOE)
            .withMessage(ERROR_MESSAGE)
            .satisfies(e -> Assertions.assertThat(e.getErrorCode()).isEqualTo(HttpURLConnection.HTTP_UNAVAILABLE));
    }

    /**
     * Test the constructor.
     */
    @Test
    void testMessageArgConstructor() {
        Assertions
            .assertThatExceptionOfType(GenieServerUnavailableException.class)
            .isThrownBy(
                () -> {
                    throw new GenieServerUnavailableException(ERROR_MESSAGE);
                }
            )
            .withNoCause()
            .withMessage(ERROR_MESSAGE)
            .satisfies(e -> Assertions.assertThat(e.getErrorCode()).isEqualTo(HttpURLConnection.HTTP_UNAVAILABLE));
    }
}
