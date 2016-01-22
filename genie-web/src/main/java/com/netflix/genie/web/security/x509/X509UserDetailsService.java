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
package com.netflix.genie.web.security.x509;

import com.netflix.genie.web.security.SecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Conditional;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.stereotype.Component;

/**
 * Get user details from an X509 Certificate Token passed in.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Conditional(SecurityConfig.OnAnySecurityEnabled.class)
@Component
public class X509UserDetailsService implements AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> {

    private static final Logger LOG = LoggerFactory.getLogger(X509UserDetailsService.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public UserDetails loadUserDetails(
        final PreAuthenticatedAuthenticationToken token
    ) throws UsernameNotFoundException {
        LOG.info("Entering loadUserDetails with token {}", token);

        throw new UsernameNotFoundException("No yet supported");
    }
}
