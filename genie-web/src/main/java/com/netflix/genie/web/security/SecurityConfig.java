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
package com.netflix.genie.web.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

import java.util.Collection;

/**
 * Primary Genie Security configuration.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Conditional(SecurityConfig.OnAnySecurityEnabled.class)
@Configuration
@EnableWebSecurity
public class SecurityConfig extends WebSecurityConfigurerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityConfig.class);

    @Autowired(required = false)
    private Collection<AuthenticationProvider> providers;

    /**
     * Global Authentication Manager.
     *
     * @return The authentication manager
     * @throws Exception
     */
    @Override
    @Bean
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return super.authenticationManagerBean();
    }

    /**
     * Configure the global authentication manager.
     *
     * @param auth The builder to configure
     */
    @Autowired
    protected void configureGlobal(final AuthenticationManagerBuilder auth) throws Exception {
        if (this.providers != null) {
            for (final AuthenticationProvider provider : this.providers) {
                LOG.debug("Adding authentication provider {} to authentication provider.", provider.toString());
                auth.authenticationProvider(provider);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final WebSecurity web) throws Exception {
        web.ignoring().antMatchers("/webjars/**", "/images/**", "/css/**", "/templates/**", "/js/**", "/vendor/**");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure(final HttpSecurity http) throws Exception {
        // This is a catch all in the case that SAML isn't turned on but one of the API security is.
        // If this isn't all the default implementation of WebSecurityConfigurerAdapter kicks in and presents
        // a default login page with the Spring Boot generated password. This allows everything through but the UI
        // won't be able to call the server to get any information. As such that configuration (SAML off but something
        // else on) is kind of pointless if you want to use the UI.
        // TODO: Revisit if there is a way to enforce this or at least provide some in memory login if nothing else
        http.antMatcher("/**").authorizeRequests().anyRequest().permitAll();
    }

    /**
     * A class used to enable the security config any time any of the supported security platforms is enabled.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class OnAnySecurityEnabled extends AnyNestedCondition {

        /**
         * Default Constructor sets the class parse time.
         */
        public OnAnySecurityEnabled() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty("security.saml.enabled")
        private static class OnSAML {
        }

        @ConditionalOnProperty("security.x509.enabled")
        private static class OnX509 {
        }

        @ConditionalOnProperty("security.oauth2.enabled")
        private static class OnOAuth2 {
        }
    }
}
