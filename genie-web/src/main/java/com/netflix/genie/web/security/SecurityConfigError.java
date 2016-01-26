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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * A class that will fire when there is an error in the configuration of security and kill the program.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Conditional({SecurityConditions.CoreSecurityEnabled.class, SecurityConditions.X509Enabled.class})
@Component
public class SecurityConfigError {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityConfigError.class);

    private final ApplicationContext context;

    /**
     * Constructor that injects the application context for use killing the application.
     *
     * @param context The Spring application context
     */
    @Autowired
    public SecurityConfigError(final ApplicationContext context) {
        this.context = context;
    }

    /**
     * Kill the program due to bad configuration.
     */
    @PostConstruct
    public void postConstruct() {
        LOG.error("Security configuration is incorrect. Can't have x509 enabled at the same time as other "
            + "configuration methods. Edit your configuration and try again.");

        SpringApplication.exit(this.context, () -> 1);
    }
}
