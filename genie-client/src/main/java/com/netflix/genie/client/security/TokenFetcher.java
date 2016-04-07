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

import com.netflix.genie.client.retrofit.TokenService;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import lombok.Getter;
import lombok.Setter;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.util.HashMap;

/**
 * Class that contains the logic to get OAuth credentials from IDP.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Getter
@Setter
public class TokenFetcher {

    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String GRANT_TYPE = "grant_type";
    private static final String SCOPE = "scope";

    // A map of all the fields needed to get the credentials
    private final HashMap<String, String> credentialParams = new HashMap<>();

    // An instance of the TokenService
    private final TokenService tokenService;

    // The url of the IDP server to get OAuth credentails
    private final String oauthUrl;

    /**
     * Constructor.
     *
     * @param oauthUrl The url of the IDP from where to get the credentials.
     * @param clientId The clientId to use to get the credentials.
     * @param clientSecret The clientSecret to use to get the credentials.
     * @param grantType The type of the grant.
     * @param scope The scope of the credentials returned.
     */
    public TokenFetcher(
        final String oauthUrl,
        final String clientId,
        final String clientSecret,
        final String grantType,
        final String scope
    ) {
        final Retrofit retrofit = new Retrofit.Builder()
            .baseUrl(oauthUrl)
            .addConverterFactory(JacksonConverterFactory.create())
            .build();

        this.oauthUrl = oauthUrl;

        // Instantiate the token service
        tokenService = retrofit.create(TokenService.class);

        // Construct the fields map to send to the IDP url.
        credentialParams.put(CLIENT_ID, clientId);
        credentialParams.put(CLIENT_SECRET, clientSecret);
        credentialParams.put(GRANT_TYPE, grantType);
        credentialParams.put(SCOPE, scope);
    }

    /**
     * Method that returns the OAuth credentials.
     *
     * @return An access token object.
     * @throws GenieException If there is any problem.
     */
    public AccessToken getToken() throws GenieException {

        // TODO set expiration time here to check it next time
        try {
            final Response<AccessToken> response = tokenService.getToken(oauthUrl, credentialParams).execute();
            if (response.isSuccessful()) {
                return response.body();
            } else {
                throw new GenieException(response.code(), "Could not fetch Token");
            }
        } catch (Exception e) {
            throw new GenieServerException("Could not get access tokens", e);
        }
    }
}
