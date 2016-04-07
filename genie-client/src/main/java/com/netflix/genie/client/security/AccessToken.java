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
package com.netflix.genie.client.security;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Class that encapsulates the OAuth credentials.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Getter
@Setter
public class AccessToken {

    // The type of the token
    @JsonProperty("token_type")
    private String tokenType;

    // The accessToken
    @JsonProperty("access_token")
    private String accessToken;

    // Time to expire from creation
    @JsonProperty("expires_in")
    private String expiresIn;
}
