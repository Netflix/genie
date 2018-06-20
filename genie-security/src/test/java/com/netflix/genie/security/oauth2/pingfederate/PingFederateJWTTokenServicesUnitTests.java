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
import com.netflix.genie.test.categories.UnitTest;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.assertj.core.util.Lists;
import org.hamcrest.Matchers;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for the PingFederateJWTTokenServices class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class PingFederateJWTTokenServicesUnitTests {

    private Timer loadAuthenticationTimer;
    private JwtConsumer jwtConsumer;
    private PingFederateJWTTokenServices tokenServices;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.loadAuthenticationTimer = Mockito.mock(Timer.class);
        final MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        Mockito.when(registry.timer(Mockito.anyString())).thenReturn(this.loadAuthenticationTimer);
        this.jwtConsumer = Mockito.mock(JwtConsumer.class);
        this.tokenServices = new PingFederateJWTTokenServices(this.jwtConsumer, registry);
    }

    /**
     * Make sure we can successfully load an authentication.
     *
     * @throws AuthenticationException On error
     * @throws InvalidTokenException   When the token is invalid
     * @throws InvalidJwtException     On invalid JWT token
     * @throws MalformedClaimException A bad claim
     */
    @Test
    public void canLoadAuthentication()
        throws AuthenticationException, InvalidTokenException, InvalidJwtException, MalformedClaimException {
        final JwtClaims claims = Mockito.mock(JwtClaims.class);
        final String clientId = UUID.randomUUID().toString();
        final String scope1 = "genie_admin";
        final String scope2 = UUID.randomUUID().toString();
        final Set<String> scopes = Sets.newHashSet(scope1, scope2);
        Mockito.when(claims.getClaimValue("client_id", String.class)).thenReturn(clientId);
        Mockito.when(claims.getClaimValue("scope", Collection.class)).thenReturn(scopes);
        Mockito.when(this.jwtConsumer.processToClaims(Mockito.anyString())).thenReturn(claims);

        final OAuth2Authentication authentication
            = this.tokenServices.loadAuthentication(UUID.randomUUID().toString());
        Assert.assertNull(authentication.getUserAuthentication());
        Assert.assertThat(authentication.getPrincipal(), Matchers.is(clientId));

        final Collection<GrantedAuthority> authorities = authentication.getAuthorities();
        Assert.assertThat(authorities.size(), Matchers.is(3));
        Assert.assertTrue(
            authorities.containsAll(
                Sets.newHashSet(
                    new SimpleGrantedAuthority("ROLE_ADMIN"),
                    new SimpleGrantedAuthority("ROLE_" + scope2.toUpperCase()),
                    new SimpleGrantedAuthority("ROLE_USER")
                )
            )
        );

        Mockito
            .verify(this.loadAuthenticationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Make sure we can't load authentication if there are no authorities.
     *
     * @throws AuthenticationException On error
     * @throws InvalidTokenException   When the token is invalid
     * @throws InvalidJwtException     On invalid JWT token
     * @throws MalformedClaimException A bad claim
     */
    @Test(expected = InvalidTokenException.class)
    public void cantLoadAuthentication()
        throws AuthenticationException, InvalidTokenException, InvalidJwtException, MalformedClaimException {
        final JwtClaims claims = Mockito.mock(JwtClaims.class);
        final String clientId = UUID.randomUUID().toString();
        final Set<String> scopes = Sets.newHashSet();
        Mockito.when(claims.getClaimValue("client_id", String.class)).thenReturn(clientId);
        Mockito.when(claims.getClaimValue("scope", Collection.class)).thenReturn(scopes);
        Mockito.when(this.jwtConsumer.processToClaims(Mockito.anyString())).thenReturn(claims);

        this.tokenServices.loadAuthentication(UUID.randomUUID().toString());
        Mockito
            .verify(this.loadAuthenticationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Make sure we can't load authentication if an exception is thrown.
     *
     * @throws AuthenticationException On error
     * @throws InvalidTokenException   When the token is invalid
     * @throws InvalidJwtException     On invalid JWT token
     * @throws MalformedClaimException A bad claim
     */
    @Test
    public void cantProcessClaims()
        throws AuthenticationException, InvalidTokenException, InvalidJwtException, MalformedClaimException {
        final JwtClaims claims = Mockito.mock(JwtClaims.class);
        Mockito.when(claims.getClaimValue("client_id", String.class))
            .thenThrow(new MalformedClaimException("bad claim"));
        Mockito.when(this.jwtConsumer.processToClaims(Mockito.anyString()))
            .thenThrow(new InvalidJwtException("bad jwt", Lists.newArrayList(), Mockito.mock(JwtContext.class)))
            .thenReturn(claims);

        try {
            this.tokenServices.loadAuthentication(UUID.randomUUID().toString());
            Assert.fail();
        } catch (final InvalidTokenException e) {
            Mockito.verify(this.jwtConsumer, Mockito.times(1)).processToClaims(Mockito.anyString());
        }

        try {
            this.tokenServices.loadAuthentication(UUID.randomUUID().toString());
            Assert.fail();
        } catch (final InvalidTokenException e) {
            Mockito.verify(this.jwtConsumer, Mockito.times(2)).processToClaims(Mockito.anyString());
        }

        Mockito
            .verify(this.loadAuthenticationTimer, Mockito.times(2))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * This method shouldn't be supported by this implementation.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void readAccessTokenNotSupported() {
        this.tokenServices.readAccessToken(UUID.randomUUID().toString());
    }
}
