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

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * Security conditions to dictate which Ping Federate configuration is activated.
 */
public class PingFederateSecurityConditions {

    /**
     * A class used to enable Ping Federate remote token services when certain conditions are met.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class PingFederateRemoteEnabled extends AllNestedConditions {

        /**
         * Default Constructor sets the class parse time.
         */
        public PingFederateRemoteEnabled() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty(value = "genie.security.oauth2.enabled", havingValue = "true")
        static class OnOAuth2 {
        }

        @ConditionalOnProperty(value = "genie.security.oauth2.pingfederate.enabled", havingValue = "true")
        static class OnPingFederate {
        }

        @ConditionalOnProperty(
            value = "genie.security.oauth2.pingfederate.jwt.enabled",
            havingValue = "false",
            matchIfMissing = true
        )
        static class OnPingFederateJwtDisabled {
        }
    }

    /**
     * A class used to enable Ping Federate JWT token services when certain conditions are met.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class PingFederateJWTEnabled extends AllNestedConditions {

        /**
         * Default Constructor sets the class parse time.
         */
        public PingFederateJWTEnabled() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty(value = "genie.security.oauth2.enabled", havingValue = "true")
        static class OnOAuth2 {
        }

        @ConditionalOnProperty(value = "genie.security.oauth2.pingfederate.enabled", havingValue = "true")
        static class OnPingFederate {
        }

        @ConditionalOnProperty(value = "genie.security.oauth2.pingfederate.jwt.enabled", havingValue = "true")
        static class OnPingFederateJwtDisabled {
        }
    }
}
