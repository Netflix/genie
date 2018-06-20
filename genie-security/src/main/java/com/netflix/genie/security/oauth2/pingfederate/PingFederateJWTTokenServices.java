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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;

import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * An implementation of the ResourceServerTokenServices interface which validates JWT tokens sent by Ping Federate.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class PingFederateJWTTokenServices implements ResourceServerTokenServices {

    private static final String ROLE = "ROLE_";
    private static final String GENIE_SCOPE_PREFIX = "genie_";
    private static final int GENIE_SCOPE_PREFIX_LENGTH = GENIE_SCOPE_PREFIX.length();

    private static final SimpleGrantedAuthority USER = new SimpleGrantedAuthority("ROLE_USER");
    private static final SimpleGrantedAuthority ADMIN = new SimpleGrantedAuthority("ROLE_ADMIN");

    private final JwtConsumer jwtConsumer;
    private final Timer loadAuthenticationTimer;

    /**
     * Constructor.
     *
     * @param jwtConsumer The JWT consumer to use
     * @param registry    The metrics registry to use
     */
    public PingFederateJWTTokenServices(
        @NotNull final JwtConsumer jwtConsumer,
        @NotNull final MeterRegistry registry
    ) {
        this.jwtConsumer = jwtConsumer;
        this.loadAuthenticationTimer = registry.timer("genie.security.oauth2.pingFederate.authentication.timer");
    }

    /**
     * Load the credentials for the specified access token.
     *
     * @param accessToken The access token value.
     * @return The authentication for the access token.
     * @throws AuthenticationException If the access token is expired
     * @throws InvalidTokenException   if the token isn't valid
     */
    @Override
    public OAuth2Authentication loadAuthentication(final String accessToken)
        throws AuthenticationException, InvalidTokenException {
        final long start = System.nanoTime();

        try {
            final JwtClaims claims = this.jwtConsumer.processToClaims(accessToken);
            log.debug("Ping Federate JWT Claims: {}", claims);
            return new OAuth2Authentication(this.getOAuth2Request(claims), null);
        } catch (final InvalidJwtException | MalformedClaimException e) {
            throw new InvalidTokenException(e.getMessage(), e);
        } finally {
            this.loadAuthenticationTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Not implemented for Genie and Ping Federate.
     */
    @Override
    public OAuth2AccessToken readAccessToken(final String accessToken) {
        throw new UnsupportedOperationException("readAccessToken not implemented");
    }

    private OAuth2Request getOAuth2Request(
        @NotNull final JwtClaims claims
    ) throws MalformedClaimException, InvalidTokenException {
        final String clientId = claims.getClaimValue("client_id", String.class);
        @SuppressWarnings("unchecked")
        final Set<String> scopes = Sets.newHashSet(claims.getClaimValue("scope", Collection.class));

        final Set<SimpleGrantedAuthority> authorities = scopes
            .stream()
            .map(
                scope -> {
                    if (scope.startsWith(GENIE_SCOPE_PREFIX)) {
                        scope = scope.substring(GENIE_SCOPE_PREFIX_LENGTH);
                    }

                    return new SimpleGrantedAuthority(ROLE + scope.toUpperCase());
                }
            )
            .collect(Collectors.toSet());

        if (authorities.isEmpty()) {
            throw new InvalidTokenException("No scopes found. Unable to authorize");
        }

        // Assume a user is a subset of admin so always grant admin user the role user as well
        if (authorities.contains(ADMIN)) {
            authorities.add(USER);
        }

        return new OAuth2Request(null, clientId, authorities, true, scopes, null, null, null, null);
    }
}
