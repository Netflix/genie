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
package com.netflix.genie.security.x509;

import com.google.common.collect.Sets;
import com.netflix.genie.security.SecurityConditions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Conditional;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Get user details from an X509 Certificate Token passed in.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Conditional(SecurityConditions.AnySecurityEnabled.class)
@Component
@Slf4j
public class X509UserDetailsService implements AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> {

    private static final String ROLE_PREFIX = "ROLE_";
    private static final GrantedAuthority USER_AUTHORITY = new SimpleGrantedAuthority(ROLE_PREFIX + "USER");

    /**
     * {@inheritDoc}
     */
    @Override
    public UserDetails loadUserDetails(
        final PreAuthenticatedAuthenticationToken token
    ) throws UsernameNotFoundException {
        log.debug("Entering loadUserDetails with token {}", token);

        // Assuming format of the principal is {username}:{role1,role2....}
        final Object principalObject = token.getPrincipal();
        if (principalObject == null || !(principalObject instanceof String)) {
            throw new UsernameNotFoundException("Expected principal to be a String");
        }

        final String principal = (String) principalObject;
        final String[] usernameAndRoles = principal.split(":");
        if (usernameAndRoles.length != 2) {
            throw new UsernameNotFoundException("User and roles not found. Must be in format {user}:{role1,role2...}");
        }

        final String username = usernameAndRoles[0];
        final String[] roles = usernameAndRoles[1].split(",");
        if (roles.length == 0) {
            throw new UsernameNotFoundException("No roles found. Unable to authenticate");
        }

        // If the certificate is valid the client is at the least a valid user
        final Set<GrantedAuthority> authorities = Sets.newHashSet(USER_AUTHORITY);
        for (final String role : roles) {
            authorities.add(new SimpleGrantedAuthority(ROLE_PREFIX + role.toUpperCase()));
        }

        final User user = new User(username, "NA", authorities);
        log.info("User {} authenticated via client certificate", user);
        return user;
    }
}
