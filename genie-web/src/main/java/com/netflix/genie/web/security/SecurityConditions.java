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
package com.netflix.genie.web.security;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.NoneNestedConditions;

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

    /**
     * A class used to enable the security config any time anything but x509 only is enabled.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class CoreSecurityEnabled extends AnyNestedCondition {

        /**
         * Default Constructor sets the class parse time.
         */
        public CoreSecurityEnabled() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty("security.saml.enabled")
        private static class OnSAML {
        }

        @ConditionalOnProperty("security.oauth2.enabled")
        private static class OnOAuth2 {
        }
    }

    /**
     * A class used to signify that the core (SAML, OAUTH2 configurations are both disabled).
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class CoreSecurityDisabled extends NoneNestedConditions {

        /**
         * Default Constructor sets the class parse time.
         */
        public CoreSecurityDisabled() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty("security.saml.enabled")
        private static class SAMLEnabled {
        }

        @ConditionalOnProperty("security.oauth2.enabled")
        private static class OAUTH2Enabled {
        }
    }

    /**
     * A class used to enable the security config any time anything but x509 only is enabled.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class X509Enabled extends AllNestedConditions {

        /**
         * Default Constructor sets the class parse time.
         */
        public X509Enabled() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty("security.x509.enabled")
        private static class X509 {
        }
    }

    /**
     * A class used to enable the security config any time anything but x509 only is enabled.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class X509Disabled extends NoneNestedConditions {

        /**
         * Default Constructor sets the class parse time.
         */
        public X509Disabled() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty("security.x509.enabled")
        private static class X509 {
        }
    }
}
