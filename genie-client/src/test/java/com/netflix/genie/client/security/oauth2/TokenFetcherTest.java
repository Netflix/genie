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
package com.netflix.genie.client.security.oauth2;

import com.netflix.genie.client.exceptions.GenieClientException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit Tests for the Token Fetcher class.
 *
 * @author amsharma
 * @since 3.0.0
 */
class TokenFetcherTest {

    private static final String URL = "http://url";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String GRANT_TYPE = "grant_type";
    private static final String SCOPE = "scope";

    /**
     * Test the constructor with URL missing.
     */
    @Test
    void testConstructorWithEmptyUrl() {
        Assertions
            .assertThatIllegalArgumentException()
            .isThrownBy(() -> new TokenFetcher(null, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE, SCOPE));
    }

    /**
     * Test the constructor with client id missing.
     */
    @Test
    void testConstructorWithEmptyClientIdl() {
        Assertions
            .assertThatIllegalArgumentException()
            .isThrownBy(() -> new TokenFetcher(URL, null, CLIENT_SECRET, GRANT_TYPE, SCOPE));
    }

    /**
     * Test the constructor with client secret missing.
     */
    @Test
    void testConstructorWithEmptyClientSecret() {
        Assertions
            .assertThatIllegalArgumentException()
            .isThrownBy(() -> new TokenFetcher(URL, CLIENT_ID, null, GRANT_TYPE, SCOPE));
    }

    /**
     * Test the constructor with grant type missing.
     */
    @Test
    void testConstructorWithEmptyGrantType() {
        Assertions
            .assertThatIllegalArgumentException()
            .isThrownBy(() -> new TokenFetcher(URL, CLIENT_ID, CLIENT_SECRET, null, SCOPE));
    }

    /**
     * Test the constructor with scope missing.
     */
    @Test
    void testConstructorWithEmptyScope() {
        Assertions
            .assertThatIllegalArgumentException()
            .isThrownBy(() -> new TokenFetcher(URL, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE, null));
    }

    /**
     * Test the constructor with malformed url.
     */
    @Test
    void testConstructorWithMalformedUrl() {
        Assertions
            .assertThatExceptionOfType(GenieClientException.class)
            .isThrownBy(() -> new TokenFetcher("foo", CLIENT_ID, CLIENT_SECRET, GRANT_TYPE, SCOPE));
    }

    /**
     * Test the constructor with valid params.
     */
    @Test
    void testConstructorWithValidParams() {
        Assertions
            .assertThatCode(() -> new TokenFetcher(URL, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE, SCOPE))
            .doesNotThrowAnyException();
    }

    /**
     * Test the getToken method for failure.
     */
    @Test
    void testGetTokenFailure() {
        Assertions
            .assertThatExceptionOfType(GenieClientException.class)
            .isThrownBy(
                () -> {
                    final TokenFetcher tokenFetcher
                        = new TokenFetcher(URL, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE, SCOPE);
                    tokenFetcher.getToken();
                }
            )
            .satisfies(e -> Assertions.assertThat(e.getErrorCode()).isEqualTo(-1));
    }
}
