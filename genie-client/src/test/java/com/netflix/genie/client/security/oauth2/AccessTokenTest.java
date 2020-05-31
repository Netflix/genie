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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit Tests for AccessToken class.
 *
 * @author amsharma
 * @since 3.0.0
 */
class AccessTokenTest {

    private AccessToken accessToken;

    @BeforeEach
    void setup() {
        this.accessToken = new AccessToken();
    }

    @Test
    void canSetAccessToken() {
        final String token = "token";
        this.accessToken.setAccessToken(token);
        Assertions.assertThat(this.accessToken.getAccessToken()).isEqualTo(token);
    }

    @Test
    void canSetTokenType() {
        final String tokenType = "token_type";
        this.accessToken.setTokenType(tokenType);
        Assertions.assertThat(this.accessToken.getTokenType()).isEqualTo(tokenType);
    }

    @Test
    void canSetExpiresIn() {
        final int expiresIn = 3600;
        this.accessToken.setExpiresIn(expiresIn);
        Assertions.assertThat(this.accessToken.getExpiresIn()).isEqualTo(expiresIn);
    }
}
