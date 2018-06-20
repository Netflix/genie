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
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.JwtContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for PingFederateValidator.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class PingFederateValidatorUnitTests {

    private Timer jwtValidationTimer;
    private PingFederateValidator validator;

    /**
     * Setup for tests.
     */
    @Before
    public void setup() {
        this.jwtValidationTimer = Mockito.mock(Timer.class);
        final MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        Mockito.when(registry.timer(Mockito.anyString())).thenReturn(this.jwtValidationTimer);
        this.validator = new PingFederateValidator(registry);
    }

    /**
     * Test to make sure a valid JWT token passes the validation.
     *
     * @throws MalformedClaimException on error
     */
    @Test
    public void canValidate() throws MalformedClaimException {
        final JwtContext context = Mockito.mock(JwtContext.class);
        final JwtClaims claims = Mockito.mock(JwtClaims.class);
        Mockito.when(context.getJwtClaims()).thenReturn(claims);
        final String clientId = UUID.randomUUID().toString();
        Mockito.when(claims.getClaimValue("client_id", String.class)).thenReturn(clientId);
        final Set<String> scopes = Sets.newHashSet(UUID.randomUUID().toString());
        Mockito.when(claims.getClaimValue("scope", Collection.class)).thenReturn(scopes);
        Assert.assertNull(this.validator.validate(context));
        Mockito
            .verify(this.jwtValidationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Test to make sure an invalid JWT token fails validation.
     *
     * @throws MalformedClaimException on error
     */
    @Test
    public void cantValidateIfNoClientId() throws MalformedClaimException {
        final JwtContext context = Mockito.mock(JwtContext.class);
        final JwtClaims claims = Mockito.mock(JwtClaims.class);
        Mockito.when(context.getJwtClaims()).thenReturn(claims);
        Mockito.when(claims.getClaimValue("client_id", String.class)).thenReturn(null);
        final Set<String> scopes = Sets.newHashSet(UUID.randomUUID().toString());
        Mockito.when(claims.getClaimValue("scope", Collection.class)).thenReturn(scopes);
        Assert.assertNotNull(this.validator.validate(context));
        Mockito
            .verify(this.jwtValidationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Test to make sure an invalid JWT token fails validation.
     *
     * @throws MalformedClaimException on error
     */
    @Test
    public void cantValidateIfNoScope() throws MalformedClaimException {
        final JwtContext context = Mockito.mock(JwtContext.class);
        final JwtClaims claims = Mockito.mock(JwtClaims.class);
        Mockito.when(context.getJwtClaims()).thenReturn(claims);
        Mockito.when(claims.getClaimValue("client_id", String.class)).thenReturn(UUID.randomUUID().toString());
        Mockito.when(claims.getClaimValue("scope", Collection.class)).thenReturn(null);
        Assert.assertNotNull(this.validator.validate(context));
        Mockito
            .verify(this.jwtValidationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Test to make sure an invalid JWT token fails validation.
     *
     * @throws MalformedClaimException on error
     */
    @Test
    public void cantValidateIfNoScopes() throws MalformedClaimException {
        final JwtContext context = Mockito.mock(JwtContext.class);
        final JwtClaims claims = Mockito.mock(JwtClaims.class);
        Mockito.when(context.getJwtClaims()).thenReturn(claims);
        Mockito.when(claims.getClaimValue("client_id", String.class)).thenReturn(UUID.randomUUID().toString());
        final Set<String> scopes = Sets.newHashSet();
        Mockito.when(claims.getClaimValue("scope", Collection.class)).thenReturn(scopes);
        Assert.assertNotNull(this.validator.validate(context));
        Mockito
            .verify(this.jwtValidationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Test to make sure an invalid JWT token fails validation.
     *
     * @throws MalformedClaimException on error
     */
    @Test
    public void cantValidateIfScopesNotStrings() throws MalformedClaimException {
        final JwtContext context = Mockito.mock(JwtContext.class);
        final JwtClaims claims = Mockito.mock(JwtClaims.class);
        Mockito.when(context.getJwtClaims()).thenReturn(claims);
        Mockito.when(claims.getClaimValue("client_id", String.class)).thenReturn(UUID.randomUUID().toString());
        final Set<Integer> scopes = Sets.newHashSet(1, 3, 5);
        Mockito.when(claims.getClaimValue("scope", Collection.class)).thenReturn(scopes);
        Assert.assertNotNull(this.validator.validate(context));
        Mockito
            .verify(this.jwtValidationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }
}
