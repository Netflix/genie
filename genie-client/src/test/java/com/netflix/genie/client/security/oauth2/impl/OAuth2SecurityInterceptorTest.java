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
package com.netflix.genie.client.security.oauth2.impl;

import okhttp3.Interceptor;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit Tests for {@link OAuth2SecurityInterceptor} Class.
 *
 * @author amsharma
 * @since 3.0.0
 */
class OAuth2SecurityInterceptorTest {

    private static final String URL = "http://localhost/foo";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String GRANT_TYPE = "grant_type";
    private static final String SCOPE = "scope";

    @Test
    void testCanConstruct() {
        Assertions
            .assertThatCode(() -> new OAuth2SecurityInterceptor(URL, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE, SCOPE))
            .doesNotThrowAnyException();
    }

    @Disabled("fails randomly")
    @Test
    void testTokenFetchFailure() throws Exception {
        final Interceptor.Chain chain = Mockito.mock(Interceptor.Chain.class);
        final OAuth2SecurityInterceptor oAuth2SecurityInterceptor = new OAuth2SecurityInterceptor(
            URL,
            CLIENT_ID,
            CLIENT_SECRET,
            GRANT_TYPE,
            SCOPE
        );

        Assertions.assertThatIOException().isThrownBy(() -> oAuth2SecurityInterceptor.intercept(chain));
    }
}
