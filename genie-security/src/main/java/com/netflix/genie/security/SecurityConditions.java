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
package com.netflix.genie.security;

import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * Container class for all the security conditions we want to use.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class SecurityConditions {

    /**
     * A class used to enable the security config any time any of the supported security platforms is enabled.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class AnySecurityEnabled extends AnyNestedCondition {

        /**
         * Default Constructor sets the class parse time.
         */
        public AnySecurityEnabled() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty(value = "genie.security.saml.enabled", havingValue = "true")
        static class OnSAML {
        }

        @ConditionalOnProperty(value = "genie.security.x509.enabled", havingValue = "true")
        static class OnX509 {
        }

        @ConditionalOnProperty(value = "genie.security.oauth2.enabled", havingValue = "true")
        static class OnOAuth2 {
        }
    }
}
