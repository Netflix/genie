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

import com.netflix.genie.client.apis.TokenService;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.net.URL;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

/**
 * Class that contains the logic to get OAuth credentials from IDP.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
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

    private Date expirationTime = new Date();

    private AccessToken accessToken;

    /**
     * Constructor.
     *
     * @param oauthUrl     The url of the IDP from where to get the credentials.
     * @param clientId     The clientId to use to get the credentials.
     * @param clientSecret The clientSecret to use to get the credentials.
     * @param grantType    The type of the grant.
     * @param scope        The scope of the credentials returned.
     * @throws GenieException If there is any problem.
     */
    public TokenFetcher(
        final String oauthUrl,
        final String clientId,
        final String clientSecret,
        final String grantType,
        final String scope
    ) throws GenieException {

        log.debug("Constructor called.");

        if (StringUtils.isBlank(oauthUrl)) {
            throw new GeniePreconditionException("URL cannot be null or empty");
        }

        if (StringUtils.isBlank(clientId)) {
            throw new GeniePreconditionException("Client Id cannot be null or empty");
        }

        if (StringUtils.isBlank(clientSecret)) {
            throw new GeniePreconditionException("Client Secret cannot be null or empty");
        }

        if (StringUtils.isBlank(grantType)) {
            throw new GeniePreconditionException("Grant Type cannot be null or empty");
        }

        if (StringUtils.isBlank(scope)) {
            throw new GeniePreconditionException("Scope cannot be null or empty");
        }

        try {
            final URL url = new URL(oauthUrl);

            // Construct the Base path of the type http[s]://serverhost/ for retrofit to work.
            final String oAuthServerUrl = url.getProtocol() + "://" + url.getHost() + "/";
            final Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(oAuthServerUrl)
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
        } catch (Exception e) {
            throw new GenieException(400, "Could not instantiate Token Service.", e);
        }
    }

    /**
     * Method that returns the OAuth credentials.
     *
     * @return An access token object.
     * @throws GenieException If there is any problem.
     */
    public AccessToken getToken() throws GenieException {

        try {
            if (new Date().after(this.expirationTime)) {
                final Response<AccessToken> response = tokenService.getToken(oauthUrl, credentialParams).execute();
                if (response.isSuccessful()) {
                    // Get current date, add expiresIn for the access token with a buffer of 5 minutes
                    final Calendar cal = Calendar.getInstance();
                    accessToken = response.body();
                    cal.add(Calendar.SECOND, accessToken.getExpiresIn() - 300);
                    this.expirationTime = cal.getTime();
                    return accessToken;
                } else {
                    throw new GenieException(response.code(), "Could not fetch Token");
                }
            } else {
                return accessToken;
            }
        } catch (Exception e) {
            throw new GenieServerException("Could not get access tokens", e);
        }
    }
}
