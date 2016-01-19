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
package com.netflix.genie.web.security.oauth2.pingfederate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.security.oauth2.resource.ResourceServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.security.oauth2.provider.token.DefaultAccessTokenConverter;

/**
 * Configuration to add beans and other components for supporting OAuth2 configuration via Ping Federate.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Conditional(PingFederateConfig.OnPingFederateEnabled.class)
@Configuration
public class PingFederateConfig {

    @Autowired
    private ResourceServerProperties resourceServerProperties;

    /**
     * When we want to use Ping Federate as our provider/authorization server.
     *
     * @return The ping federate configuration.
     */
    @Bean
    @Primary
    public PingFederateTokenServices pingFederateTokenServices() {
        final DefaultAccessTokenConverter converter = new DefaultAccessTokenConverter();
        converter.setUserTokenConverter(new PingFederateUserAuthenticationConverter());
        return new PingFederateTokenServices(this.resourceServerProperties, converter);
    }

    /**
     * A class used to enable the Ping Federate based configuration any time both OAuth2 and Ping Federate are enabled.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class OnPingFederateEnabled extends AllNestedConditions {

        /**
         * Default Constructor sets the class parse time.
         */
        public OnPingFederateEnabled() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty("security.oauth2.enabled")
        private static class OnOAuth2 {
        }

        @ConditionalOnProperty("security.oauth2.pingfederate.enabled")
        private static class OnPingFederate {
        }
    }
}
