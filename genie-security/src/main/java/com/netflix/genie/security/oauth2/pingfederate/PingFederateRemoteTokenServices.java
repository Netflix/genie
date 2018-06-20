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
package com.netflix.genie.security.oauth2.pingfederate;

import com.google.common.collect.Sets;
import com.netflix.genie.web.util.MetricsConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.security.oauth2.resource.ResourceServerProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.AccessTokenConverter;
import org.springframework.security.oauth2.provider.token.RemoteTokenServices;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A remote token services extension for Ping Federate based IDPs.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class PingFederateRemoteTokenServices extends RemoteTokenServices {

    protected static final String CLIENT_ID_KEY = "client_id";
    protected static final String ERROR_KEY = "error";
    protected static final String SCOPE_KEY = "scope";
    protected static final String AUTHENTICATION_TIMER_NAME = "genie.security.oauth2.pingFederate.authentication.timer";
    protected static final String API_TIMER_NAME = "genie.security.oauth2.pingFederate.api.timer";
    static final String TOKEN_VALIDATION_ERROR_COUNTER_NAME
        = "genie.security.oauth2.pingFederate.tokenValidation.error.rate";
    private static final String TOKEN_NAME_KEY = "token";
    private static final String CLIENT_SECRET_KEY = "client_secret";
    private static final String GRANT_TYPE_KEY = "grant_type";
    private static final String GRANT_TYPE = "urn:pingidentity.com:oauth2:grant_type:validate_bearer";
    private final AccessTokenConverter converter;
    private final String checkTokenEndpointUrl;
    private final String clientId;
    private final String clientSecret;
    // Metrics
    private final Timer authenticationTimer;
    private final Timer pingFederateAPITimer;
    private RestTemplate localRestTemplate;

    /**
     * Constructor.
     *
     * @param serverProperties The properties of the resource server (Genie)
     * @param converter        The access token converter to use
     * @param registry         The metrics registry to use
     */
    public PingFederateRemoteTokenServices(
        @NotNull final ResourceServerProperties serverProperties,
        @NotNull final AccessTokenConverter converter,
        @NotNull final MeterRegistry registry
    ) {
        super();
        this.authenticationTimer = registry.timer(AUTHENTICATION_TIMER_NAME);
        this.pingFederateAPITimer = registry.timer(API_TIMER_NAME);
        final HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(10000);
        final RestTemplate restTemplate = new RestTemplate(factory);
        final List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
        interceptors.add(
            (final HttpRequest request, final byte[] body, final ClientHttpRequestExecution execution) -> {
                final long start = System.nanoTime();
                try {
                    return execution.execute(request, body);
                } finally {
                    pingFederateAPITimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                }
            }
        );
        restTemplate.setInterceptors(interceptors);
        restTemplate.setErrorHandler(
            new DefaultResponseErrorHandler() {
                // Ignore 400
                @Override
                public void handleError(final ClientHttpResponse response) throws IOException {
                    final int errorCode = response.getRawStatusCode();
                    registry
                        .counter(
                            TOKEN_VALIDATION_ERROR_COUNTER_NAME,
                            Sets.newHashSet(Tag.of(MetricsConstants.TagKeys.STATUS, Integer.toString(errorCode)))
                        )
                        .increment();
                    if (response.getRawStatusCode() != HttpStatus.BAD_REQUEST.value()) {
                        super.handleError(response);
                    }
                }
            }
        );

        this.setRestTemplate(restTemplate);

        this.checkTokenEndpointUrl = serverProperties.getTokenInfoUri();
        this.clientId = serverProperties.getClientId();
        this.clientSecret = serverProperties.getClientSecret();

        Assert.state(StringUtils.isNotBlank(this.checkTokenEndpointUrl), "Check Endpoint URL is required");
        Assert.state(StringUtils.isNotBlank(this.clientId), "Client ID is required");
        Assert.state(StringUtils.isNotBlank(this.clientSecret), "Client secret is required");

        log.debug("checkTokenEndpointUrl = {}", this.checkTokenEndpointUrl);
        log.debug("clientId = {}", this.clientId);
        log.debug("clientSecret = {}", this.clientSecret);

        this.converter = converter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OAuth2Authentication loadAuthentication(final String accessToken)
        throws AuthenticationException, InvalidTokenException {
        final long start = System.nanoTime();
        try {
            final MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
            formData.add(TOKEN_NAME_KEY, accessToken);
            formData.add(CLIENT_ID_KEY, this.clientId);
            formData.add(CLIENT_SECRET_KEY, this.clientSecret);
            formData.add(GRANT_TYPE_KEY, GRANT_TYPE);

            final Map<String, Object> map = this.postForMap(this.checkTokenEndpointUrl, formData);

            if (map.containsKey(ERROR_KEY)) {
                final String error = map.get(ERROR_KEY).toString();
                log.debug("Validating the token produced an error: {}", error);
                throw new InvalidTokenException(error);
            }

            Assert.state(map.containsKey(CLIENT_ID_KEY), "Client id must be present in response from auth server");
            Assert.state(map.containsKey(SCOPE_KEY), "No scopes included in response from authentication server");
            this.convertScopes(map);
            final OAuth2Authentication authentication = this.converter.extractAuthentication(map);
            log.info(
                "User {} authenticated with authorities {}",
                authentication.getPrincipal(),
                authentication.getAuthorities()
            );
            return authentication;
        } finally {
            final long finished = System.nanoTime();
            this.authenticationTimer.record(finished - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Set the rest operations to use.
     *
     * @param restTemplate The rest operations to use. Not null.
     */
    protected void setRestTemplate(@NotNull final RestTemplate restTemplate) {
        super.setRestTemplate(restTemplate);
        this.localRestTemplate = restTemplate;
    }

    private Map<String, Object> postForMap(final String path, final MultiValueMap<String, String> formData) {
        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        @SuppressWarnings("rawtypes") final Map map = this.localRestTemplate.exchange(
            path, HttpMethod.POST, new HttpEntity<>(formData, headers), Map.class
        ).getBody();
        @SuppressWarnings("unchecked") final Map<String, Object> result = map;
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
