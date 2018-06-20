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
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;
import org.springframework.security.oauth2.provider.token.DefaultUserAuthenticationConverter;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Class to convert a map received from Ping Federate to a Spring Authentication object.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class PingFederateUserAuthenticationConverter extends DefaultUserAuthenticationConverter {

    protected static final String CLIENT_ID_KEY = "client_id";
    protected static final String SCOPE_KEY = "scope";
    protected static final String GENIE_PREFIX = "genie_";
    protected static final String ROLE_PREFIX = "ROLE_";
    private static final GrantedAuthority USER_AUTHORITY = new SimpleGrantedAuthority("ROLE_USER");

    /**
     * {@inheritDoc}
     */
    //TODO: might be too much unnecessary validation in here
    @Override
    public Authentication extractAuthentication(final Map<String, ?> map) {
        // Make sure we have a client id to use as the Principle
        if (!map.containsKey(CLIENT_ID_KEY)) {
            throw new InvalidTokenException("No client id key found in map");
        }

        final Object clientIdObject = map.get(CLIENT_ID_KEY);
        if (!(clientIdObject instanceof String)) {
            throw new InvalidTokenException("Client id wasn't string");
        }

        final String userName = (String) clientIdObject;
        if (StringUtils.isBlank(userName)) {
            throw new InvalidTokenException("Client id was blank. Unable to use as user name");
        }

        // Scopes were already validated in PingFederateRemoteTokenServices
        final Object scopeObject = map.get(SCOPE_KEY);
        if (!(scopeObject instanceof Collection)) {
            throw new InvalidTokenException("Scopes were not a collection");
        }

        @SuppressWarnings("unchecked")
        final Collection<String> scopes = (Collection<String>) scopeObject;
        if (scopes.isEmpty()) {
            throw new InvalidTokenException("No scopes available. Unable to authenticate");
        }

        // Default to user role
        final Set<GrantedAuthority> authorities = Sets.newHashSet(USER_AUTHORITY);

        scopes
            .stream()
            .filter(scope -> scope.contains(GENIE_PREFIX))
            .distinct()
            .forEach(
                scope -> authorities.add(
                    new SimpleGrantedAuthority(
                        ROLE_PREFIX + StringUtils.removeStartIgnoreCase(scope, GENIE_PREFIX).toUpperCase()
                    )
                )
            );

        return new UsernamePasswordAuthenticationToken(userName, "N/A", authorities);
    }
}
