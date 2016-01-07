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
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;

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
@EnableWebSecurity
@EnableResourceServer
public class OAuth2Config /* extends ResourceServerConfigurerAdapter */ {

//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public void configure(final ResourceServerSecurityConfigurer resources) throws Exception {
//        super.configure(resources);
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public void configure(final HttpSecurity http) throws Exception {
//        super.configure(http);
//    }
//
//    /**
//     * blah.
//     *
//     * @param checkTokenUrl blah
//     * @param clientId blah
//     * @param clientSecret blah
//     * @return who cares
//     */
//    @Bean
//    public RemoteTokenServices remoteTokenServices(
//        @Value("${auth.server.url}") final String checkTokenUrl,
//        @Value("${auth.server.clientId}") final String clientId,
//        @Value("${auth.server.clientsecret}") final String clientSecret
//    ) {
//        final RemoteTokenServices remoteTokenServices = new RemoteTokenServices();
//        remoteTokenServices.setCheckTokenEndpointUrl(checkTokenUrl);
//        remoteTokenServices.setClientId(clientId);
//        remoteTokenServices.setClientSecret(clientSecret);
//        return remoteTokenServices;
//    }
}
