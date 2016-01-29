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
package com.netflix.genie.web.security.oauth2.pingfederate;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.security.oauth2.resource.ResourceServerProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;
import org.springframework.security.oauth2.provider.token.AccessTokenConverter;
import org.springframework.security.oauth2.provider.token.DefaultAccessTokenConverter;
import org.springframework.web.client.RestOperations;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for PingFederateTokenServices.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class PingFederateTokenServicesUnitTests {

    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final String CLIENT_SECRET = UUID.randomUUID().toString();
    private static final String CHECK_TOKEN_ENDPOINT_URL = UUID.randomUUID().toString();

    private ResourceServerProperties resourceServerProperties;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.resourceServerProperties = new ResourceServerProperties(CLIENT_ID, CLIENT_SECRET);
        this.resourceServerProperties.setTokenInfoUri(CHECK_TOKEN_ENDPOINT_URL);
    }

    /**
     * Test to make sure valid input to the constructor works.
     */
    @Test
    public void canConstruct() {
        final AccessTokenConverter converter = new DefaultAccessTokenConverter();
        final PingFederateTokenServices services
            = new PingFederateTokenServices(this.resourceServerProperties, converter);

        Assert.assertThat(services.getAccessTokenConverter(), Matchers.is(converter));
        Assert.assertThat(services.getCheckTokenEndpointUrl(), Matchers.is(CHECK_TOKEN_ENDPOINT_URL));
        Assert.assertThat(services.getClientId(), Matchers.is(CLIENT_ID));
        Assert.assertThat(services.getClientSecret(), Matchers.is(CLIENT_SECRET));
        Assert.assertNotNull(services.getRestOperations());
    }

    /**
     * Make sure we can't construct without a client id.
     */
    @Test(expected = IllegalStateException.class)
    public void cantConstructWithoutClientId() {
        final ResourceServerProperties properties = new ResourceServerProperties(null, null);
        final AccessTokenConverter converter = new DefaultAccessTokenConverter();
        new PingFederateTokenServices(properties, converter);
    }

    /**
     * Make sure we can't construct without a client secret.
     */
    @Test(expected = IllegalStateException.class)
    public void cantConstructWithoutClientSecret() {
        final ResourceServerProperties properties = new ResourceServerProperties("AnID", null);
        final AccessTokenConverter converter = new DefaultAccessTokenConverter();
        new PingFederateTokenServices(properties, converter);
    }

    /**
     * Make sure we can't construct without a check token url.
     */
    @Test(expected = IllegalStateException.class)
    public void cantConstructWithoutCheckTokenURL() {
        this.resourceServerProperties.setTokenInfoUri(null);
        final AccessTokenConverter converter = new DefaultAccessTokenConverter();
        new PingFederateTokenServices(this.resourceServerProperties, converter);
    }

    /**
     * Make sure we can validate a token.
     */
    @Test
    public void canLoadAuthentication() {
        final AccessTokenConverter converter = Mockito.mock(AccessTokenConverter.class);
        final RestOperations restOperations = Mockito.mock(RestOperations.class);
        final PingFederateTokenServices services
            = new PingFederateTokenServices(this.resourceServerProperties, converter);
        services.setRestOperations(restOperations);
        final String accessToken = UUID.randomUUID().toString();

        final String clientId = UUID.randomUUID().toString();
        final String scope1 = UUID.randomUUID().toString();
        final String scope2 = UUID.randomUUID().toString();

        final Map<String, Object> map = Maps.newHashMap();
        map.put(PingFederateTokenServices.CLIENT_ID_KEY, clientId);
        map.put(
            PingFederateTokenServices.SCOPE_KEY,
            scope1 + " " + scope2
        );

        @SuppressWarnings("unchecked")
        final ResponseEntity<Map> response = Mockito.mock(ResponseEntity.class);

        Mockito.when(
            restOperations.exchange(
                Mockito.eq(CHECK_TOKEN_ENDPOINT_URL),
                Mockito.eq(HttpMethod.POST),
                Mockito.any(HttpEntity.class),
                Mockito.eq(Map.class)
            )
        ).thenReturn(response);

        Mockito.when(response.getBody()).thenReturn(map);

        final SimpleGrantedAuthority scope1Authority = new SimpleGrantedAuthority(scope1);
        final SimpleGrantedAuthority scope2Authority = new SimpleGrantedAuthority(scope2);
        final Set<GrantedAuthority> authorities
            = Sets.newHashSet(scope1Authority, scope2Authority);

        final Authentication authentication = new UsernamePasswordAuthenticationToken(
            clientId,
            "NA",
            authorities
        );

        final OAuth2Authentication oauth2Authentication
            = new OAuth2Authentication(Mockito.mock(OAuth2Request.class), authentication);

        Mockito.when(
            converter.extractAuthentication(Mockito.eq(map))
        ).thenReturn(oauth2Authentication);

        final OAuth2Authentication result = services.loadAuthentication(accessToken);
        Assert.assertThat(result, Matchers.is(oauth2Authentication));
        Assert.assertThat(result.getPrincipal(), Matchers.is(clientId));
        Assert.assertThat(result.getAuthorities().size(), Matchers.is(2));
        Assert.assertTrue(result.getAuthorities().contains(scope1Authority));
        Assert.assertTrue(result.getAuthorities().contains(scope2Authority));
    }

    /**
     * Make sure invalid response from server causes authentication to fail.
     */
    @Test(expected = InvalidTokenException.class)
    public void cantLoadAuthenticationOnError() {
        final AccessTokenConverter converter = Mockito.mock(AccessTokenConverter.class);
        final RestOperations restOperations = Mockito.mock(RestOperations.class);
        final PingFederateTokenServices services
            = new PingFederateTokenServices(this.resourceServerProperties, converter);
        services.setRestOperations(restOperations);
        final String accessToken = UUID.randomUUID().toString();

        final Map<String, Object> map = Maps.newHashMap();
        map.put(PingFederateTokenServices.ERROR_KEY, UUID.randomUUID().toString());

        @SuppressWarnings("unchecked")
        final ResponseEntity<Map> response = Mockito.mock(ResponseEntity.class);

        Mockito.when(
            restOperations.exchange(
                Mockito.eq(CHECK_TOKEN_ENDPOINT_URL),
                Mockito.eq(HttpMethod.POST),
                Mockito.any(HttpEntity.class),
                Mockito.eq(Map.class)
            )
        ).thenReturn(response);

        Mockito.when(response.getBody()).thenReturn(map);

        // Should throw exception based on error key being present
        services.loadAuthentication(accessToken);
    }

    /**
     * This method isn't implemented for Ping Federate currently. Make sure this fails in case we ever implement it
     * and need to update the tests.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void cantReadAccessToken() {
        final AccessTokenConverter converter = new DefaultAccessTokenConverter();
        final PingFederateTokenServices services
            = new PingFederateTokenServices(this.resourceServerProperties, converter);
        services.readAccessToken(UUID.randomUUID().toString());
    }
}
