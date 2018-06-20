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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.JwtContext;
import org.jose4j.jwt.consumer.Validator;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * A validator for Ping Federate generated JWT tokens.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class PingFederateValidator implements Validator {

    private final Timer jwtValidationTimer;

    /**
     * Constructor.
     *
     * @param registry The registry to use
     */
    public PingFederateValidator(final MeterRegistry registry) {
        this.jwtValidationTimer = registry.timer("genie.security.oauth2.pingFederate.jwt.validation.timer");
    }

    /**
     * Make sure the JWT has the claims we expect to exist.
     *
     * @param jwtContext the JWT context
     * @return a description of the problem or null, if valid
     * @throws MalformedClaimException if a malformed claim is encountered
     */
    @Override
    public String validate(final JwtContext jwtContext) throws MalformedClaimException {
        final long start = System.nanoTime();
        try {
            final JwtClaims claims = jwtContext.getJwtClaims();
            final StringBuilder builder = new StringBuilder();
            final String clientId = claims.getClaimValue("client_id", String.class);
            if (clientId == null) {
                builder.append("No client_id field present and is required. ");
            }
            final Collection<?> scopes = claims.getClaimValue("scope", Collection.class);
            if (scopes == null) {
                builder.append("No scope claim present and is required. ");
            } else if (scopes.isEmpty()) {
                builder.append("No scopes present at least one required. ");
            } else if (!(scopes.iterator().next() instanceof String)) {
                builder.append("Scopes must be of type string. ");
            }

            if (builder.length() == 0) {
                return null;
            } else {
                return builder.toString();
            }
        } finally {
            this.jwtValidationTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
}
