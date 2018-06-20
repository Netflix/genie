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
package com.netflix.genie.security.x509;

import com.netflix.genie.security.SecurityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

/**
 * Spring Security configuration based on authentication of x509 certificates only.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConditionalOnProperty(value = "genie.security.x509.enabled", havingValue = "true")
@Configuration
@Order(4)
public class X509Config extends WebSecurityConfigurerAdapter {

    @Autowired
    private X509UserDetailsService x509UserDetailsService;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure(final HttpSecurity http) throws Exception {
        SecurityUtils.buildAPIHttpSecurity(http, this.x509UserDetailsService);
    }
}
