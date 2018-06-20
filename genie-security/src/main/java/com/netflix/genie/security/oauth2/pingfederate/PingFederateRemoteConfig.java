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

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.security.oauth2.resource.ResourceServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.security.oauth2.provider.token.DefaultAccessTokenConverter;
import org.springframework.security.oauth2.provider.token.UserAuthenticationConverter;

import javax.validation.constraints.NotNull;

/**
 * Configuration to add beans and other components for supporting OAuth2 authentication via Ping Federate remote
 * API calls.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@Conditional(PingFederateSecurityConditions.PingFederateRemoteEnabled.class)
public class PingFederateRemoteConfig {

    /**
     * The class to convert the response from Ping Federate to an authentication object in Spring Security.
     *
     * @return Instance of PingFederateUserAuthenticationConverter
     */
    @Bean
    public PingFederateUserAuthenticationConverter pingFederateUserAuthenticationConverter() {
        return new PingFederateUserAuthenticationConverter();
    }

    /**
     * The class used to covert access tokens to authentications in Spring Security.
     *
     * @param userAuthenticationConverter The user converter to use
     * @return A DefaultAccessTokenConverter with the ping federate user authentication converter class used
     */
    @Bean
    public DefaultAccessTokenConverter defaultAccessTokenConverter(
        @NotNull final UserAuthenticationConverter userAuthenticationConverter
    ) {
        final DefaultAccessTokenConverter converter = new DefaultAccessTokenConverter();
        converter.setUserTokenConverter(userAuthenticationConverter);
        return converter;
    }

    /**
     * When we want to use Ping Federate as our provider/authorization server.
     *
     * @param converter                The access token converter to use
     * @param resourceServerProperties The properties to use to configure the token services
     * @param registry                 The metrics registry to use
     * @return The ping federate configuration.
     */
    @Bean
    @Primary
    public PingFederateRemoteTokenServices pingFederateTokenServices(
        final DefaultAccessTokenConverter converter,
        final ResourceServerProperties resourceServerProperties,
        final MeterRegistry registry
    ) {
        return new PingFederateRemoteTokenServices(resourceServerProperties, converter, registry);
    }
}
