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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the PingFederateUserAuthenticationConverter.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class PingFederateUserAuthenticationConverterUnitTests {

    private PingFederateUserAuthenticationConverter converter;
    private Map<String, Object> map;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.converter = new PingFederateUserAuthenticationConverter();
        this.map = Maps.newHashMap();
    }

    /**
     * Make sure that without a client id no authentication can be derived.
     */
    @Test(expected = InvalidTokenException.class)
    public void cantGetAuthenticationWithoutClientId() {
        this.converter.extractAuthentication(this.map);
    }

    /**
     * Make sure that without a string client id no authentication can be derived.
     */
    @Test(expected = InvalidTokenException.class)
    public void cantGetAuthenticationWithoutStringClientId() {
        this.map.put(PingFederateUserAuthenticationConverter.CLIENT_ID_KEY, Boolean.TRUE);
        this.converter.extractAuthentication(this.map);
    }

    /**
     * Make sure that without a string client id no authentication can be derived.
     */
    @Test(expected = InvalidTokenException.class)
    public void cantGetAuthenticationWithBlankClientId() {
        this.map.put(PingFederateUserAuthenticationConverter.CLIENT_ID_KEY, "");
        this.converter.extractAuthentication(this.map);
    }

    /**
     * Make sure that without any scopes no authentication can be derived.
     */
    @Test(expected = InvalidTokenException.class)
    public void cantGetAuthenticationWithoutScope() {
        this.map.put(PingFederateUserAuthenticationConverter.CLIENT_ID_KEY, UUID.randomUUID().toString());
        this.map.put(PingFederateUserAuthenticationConverter.SCOPE_KEY, "Not a Collection");
        this.converter.extractAuthentication(this.map);
    }

    /**
     * Make sure that without any scopes no authentication can be derived.
     */
    @Test(expected = InvalidTokenException.class)
    public void cantGetAuthenticationWithoutAnyScopes() {
        this.map.put(PingFederateUserAuthenticationConverter.CLIENT_ID_KEY, UUID.randomUUID().toString());
        this.map.put(PingFederateUserAuthenticationConverter.SCOPE_KEY, new HashSet<String>());
        this.converter.extractAuthentication(this.map);
    }

    /**
     * Make sure that with all the require elements we can authenticate.
     */
    @Test
    public void canAuthenticateUser() {
        final String clientId = UUID.randomUUID().toString();
        final Set<String> scopes = Sets.newHashSet(PingFederateUserAuthenticationConverter.GENIE_PREFIX + "user");
        this.map.put(PingFederateUserAuthenticationConverter.CLIENT_ID_KEY, clientId);
        this.map.put(PingFederateUserAuthenticationConverter.SCOPE_KEY, scopes);
        final Authentication authentication = this.converter.extractAuthentication(this.map);

        Assert.assertTrue(authentication instanceof UsernamePasswordAuthenticationToken);
        Assert.assertThat(authentication.getPrincipal(), Matchers.is(clientId));
        Assert.assertThat(authentication.getAuthorities().size(), Matchers.is(1));
        Assert.assertThat(authentication.getAuthorities(), Matchers.contains(new SimpleGrantedAuthority("ROLE_USER")));
    }

    /**
     * Make sure that with all the require elements we can authenticate an admin.
     */
    @Test
    public void canAuthenticateAdmin() {
        final String clientId = UUID.randomUUID().toString();
        final Set<String> scopes = Sets.newHashSet(PingFederateUserAuthenticationConverter.GENIE_PREFIX + "admin");
        this.map.put(PingFederateUserAuthenticationConverter.CLIENT_ID_KEY, clientId);
        this.map.put(PingFederateUserAuthenticationConverter.SCOPE_KEY, scopes);
        final Authentication authentication = this.converter.extractAuthentication(this.map);

        Assert.assertTrue(authentication instanceof UsernamePasswordAuthenticationToken);
        Assert.assertThat(authentication.getPrincipal(), Matchers.is(clientId));
        Assert.assertThat(authentication.getAuthorities().size(), Matchers.is(2));
        Assert.assertThat(
            authentication.getAuthorities(),
            Matchers.containsInAnyOrder(
                new SimpleGrantedAuthority("ROLE_ADMIN"),
                new SimpleGrantedAuthority("ROLE_USER")
            )
        );
    }
}
