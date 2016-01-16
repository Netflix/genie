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
package com.netflix.genie.web.security.oauth2;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;

/**
 * Security Configuration for OAuth2.
 * <p>
 * https://youtu.be/MLfL1NpwUC4?t=1h7m55s
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConditionalOnProperty("security.oauth2.enabled")
@Configuration
@EnableResourceServer
//@EnableOAuth2Sso
public class OAuth2Config extends ResourceServerConfigurerAdapter {

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final ResourceServerSecurityConfigurer resources) throws Exception {
//        resources.tokenServices(new PingFederateTokenServices()).stateless(true);
        resources.stateless(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final HttpSecurity http) throws Exception {
        // @formatter:off
        http
            .antMatcher("/api/**")
                .authorizeRequests()
                    .anyRequest().authenticated()
            .and()
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED);
        // @formatter:on
    }
}
