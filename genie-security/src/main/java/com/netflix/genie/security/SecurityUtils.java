/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.security;

import com.netflix.genie.security.x509.X509UserDetailsService;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.savedrequest.NullRequestCache;

import javax.validation.constraints.NotNull;

/**
 * Utility methods for common functionality in security configurations that ONLY configure security on API paths.
 *
 * @author tgianos
 * @since 3.0.0
 */
public final class SecurityUtils {

    private static final String ADMIN_ROLE = "ADMIN";
    private static final String USER_ROLE = "USER";
    private static final String APPLICATIONS_API_REGEX = "/api/.*/applications.*";
    private static final String CLUSTERS_API_REGEX = "/api/.*/clusters.*";
    private static final String COMMANDS_API_REGEX = "/api/.*/commands.*";

    /**
     * Protected constructor for utility class.
     */
    protected SecurityUtils() {
    }

    /**
     * Build the common API HTTP security.
     *
     * @param http                   The http security object to use
     * @param x509UserDetailsService The x509 authentication user details service to use
     * @throws Exception when there is a problem configuring HTTP errors
     */
    public static void buildAPIHttpSecurity(
        @NotNull final HttpSecurity http,
        @NotNull final X509UserDetailsService x509UserDetailsService
    ) throws Exception {
        // @formatter:off
        http
            .regexMatcher("(/api/.*)")
                .authorizeRequests()
                    .regexMatchers(HttpMethod.DELETE, APPLICATIONS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.PATCH, APPLICATIONS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.POST, APPLICATIONS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.PUT, APPLICATIONS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.DELETE, CLUSTERS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.PATCH, CLUSTERS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.POST, CLUSTERS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.PUT, CLUSTERS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.DELETE, COMMANDS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.PATCH, COMMANDS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.POST, COMMANDS_API_REGEX).hasRole(ADMIN_ROLE)
                    .regexMatchers(HttpMethod.PUT, COMMANDS_API_REGEX).hasRole(ADMIN_ROLE)
                    .anyRequest().hasRole(USER_ROLE)
            .and()
                .x509().authenticationUserDetailsService(x509UserDetailsService)
            .and()
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.NEVER)
//            .and()
//                .requiresChannel().anyRequest().requiresSecure()
            .and()
                .requestCache().requestCache(new NullRequestCache())
            .and()
                .csrf().disable();
        // @formatter:on
    }
}
