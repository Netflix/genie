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

import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

/**
 * Primary Genie Security configuration.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Conditional(SecurityConfig.OnAnySecurityEnabled.class)
@Configuration
@EnableWebSecurity
public class SecurityConfig {

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
