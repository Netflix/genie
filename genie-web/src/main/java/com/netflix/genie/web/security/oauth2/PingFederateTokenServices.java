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
package com.netflix.genie.web.security.oauth2;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.security.oauth2.resource.ResourceServerProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.AccessTokenConverter;
import org.springframework.security.oauth2.provider.token.DefaultAccessTokenConverter;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * A remote token services extension for Ping Federate based IDPs.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class PingFederateTokenServices implements ResourceServerTokenServices {

    private static final Logger LOG = LoggerFactory.getLogger(PingFederateTokenServices.class);
    private static final String TOKEN_NAME_KEY = "token";
    private static final String CLIENT_ID_KEY = "client_id";
    private static final String CLIENT_SECRET_KEY = "client_secret";
    private static final String GRANT_TYPE_KEY = "grant_type";
    private static final String ERROR_KEY = "error";
    private static final String SCOPE_KEY = "scope";
    private static final String GRANT_TYPE = "urn:pingidentity.com:oauth2:grant_type:validate_bearer";

    private final AccessTokenConverter converter = new DefaultAccessTokenConverter();

    private final String checkTokenEndpointUrl;
    private final String clientId;
    private final String clientSecret;
    private final RestOperations restOperations;

    /**
     * Constructor.
     *
     * @param serverProperties The properties of the resource server (Genie)
     */
    public PingFederateTokenServices(
        final ResourceServerProperties serverProperties
    ) {
        this.restOperations = new RestTemplate();
        ((RestTemplate) this.restOperations).setErrorHandler(new DefaultResponseErrorHandler() {
            @Override
            // Ignore 400
            public void handleError(final ClientHttpResponse response) throws IOException {
                if (response.getRawStatusCode() != HttpStatus.BAD_REQUEST.value()) {
                    super.handleError(response);
                }
            }
        });

        this.checkTokenEndpointUrl = serverProperties.getTokenInfoUri();
        this.clientId = serverProperties.getClientId();
        this.clientSecret = serverProperties.getClientSecret();

        LOG.info("checkTokenEnpointUrl = {}", this.checkTokenEndpointUrl);
        LOG.info("clientId = {}", this.clientId);
        LOG.info("clientSecret = {}", this.clientSecret);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OAuth2Authentication loadAuthentication(final String accessToken)
        throws AuthenticationException, InvalidTokenException {
        final MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
        formData.add(TOKEN_NAME_KEY, accessToken);
        formData.add(CLIENT_ID_KEY, this.clientId);
        formData.add(CLIENT_SECRET_KEY, this.clientSecret);
        formData.add(GRANT_TYPE_KEY, GRANT_TYPE);

        final Map<String, Object> map = this.postForMap(this.checkTokenEndpointUrl, formData);

        if (map.containsKey(ERROR_KEY)) {
            LOG.debug("Validating the token produced an error: {}", map.get(ERROR_KEY));
            throw new InvalidTokenException(accessToken);
        }

        Assert.state(map.containsKey(CLIENT_ID_KEY), "Client id must be present in response from auth server");
        Assert.state(map.containsKey(SCOPE_KEY), "No scopes included in response from authentication server");
        this.convertScopes(map);
        return this.converter.extractAuthentication(map);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OAuth2AccessToken readAccessToken(final String accessToken) {
        throw new UnsupportedOperationException("readAccessToken not implemented for Ping Federate");
    }

    private Map<String, Object> postForMap(
        final String path,
        final MultiValueMap<String, String> formData
    ) {
        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        @SuppressWarnings("rawtypes")
        final Map map = this.restOperations.exchange(
            path, HttpMethod.POST, new HttpEntity<>(formData, headers), Map.class
        ).getBody();
        @SuppressWarnings("unchecked")
        final Map<String, Object> result = map;
        return result;
    }

    private void convertScopes(final Map<String, Object> oauth2Map) {
        final Object scopesObject = oauth2Map.get(SCOPE_KEY);
        if (scopesObject == null) {
            throw new InvalidTokenException("Scopes were null");
        }

        if (scopesObject instanceof String) {
            final String scopes = (String) scopesObject;
            if (StringUtils.isBlank(scopes)) {
                throw new InvalidTokenException("No scopes found unable to authenticate");
            }

            oauth2Map.put(SCOPE_KEY, Arrays.asList(StringUtils.split(scopes, ' ')));
        } else {
            throw new InvalidTokenException("Scopes was not a String");
        }
    }
}
