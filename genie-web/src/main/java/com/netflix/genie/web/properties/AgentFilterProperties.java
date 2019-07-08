/*
 *
 *  Copyright 2019 Netflix, Inc.
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

package com.netflix.genie.web.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Properties for the {@link com.netflix.genie.web.services.AgentFilterService}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = AgentFilterProperties.PROPERTY_PREFIX)
@Validated
public class AgentFilterProperties implements EnvironmentAware {

    /**
     * Prefix for the properties that will be bound into this object.
     */
    static final String PROPERTY_PREFIX = "genie.agent.filter";

    /**
     * Property that enables the default implementation of {@link com.netflix.genie.web.services.AgentFilterService}.
     */
    public static final String VERSION_FILTER_ENABLED_PROPERTY = PROPERTY_PREFIX + ".enabled";
    private static final String MINIMUM_VERSION_PROPERTY = PROPERTY_PREFIX + ".version.minimum";
    private static final String BLACKLISTED_VERSION_REGEX_PROPERTY = PROPERTY_PREFIX + ".version.blacklist";
    private static final String WHITELISTED_VERSION_REGEX_PROPERTY = PROPERTY_PREFIX + ".version.whitelist";

    private AtomicReference<Environment> environment = new AtomicReference<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEnvironment(final Environment environment) {
        this.environment.set(environment);
    }

    /**
     * Get the (dynamic) property value representing the minimum agent version allowed to connect.
     *
     * @return a version string or null if a minimum is not active
     */
    @Nullable
    public String getMinimumVersion() {
        return getValueOrNull(MINIMUM_VERSION_PROPERTY);

    }

    /**
     * Get the (dynamic) property value containing a regular expression used to blacklist agent versions.
     *
     * @return a string containing a regular expression pattern, or null if one is not set
     */
    @Nullable
    public String getBlacklistedVersions() {
        return getValueOrNull(BLACKLISTED_VERSION_REGEX_PROPERTY);
    }

    /**
     * Get the (dynamic) property value containing a regular expression used to whitelist agent versions.
     *
     * @return a string containing a regular expression pattern, or null if one is not set
     */
    @Nullable
    public String getWhitelistedVersions() {
        return getValueOrNull(WHITELISTED_VERSION_REGEX_PROPERTY);
    }

    private String getValueOrNull(final String propertyKey) {
        final Environment env = this.environment.get();
        if (env != null) {
            return env.getProperty(propertyKey);
        }
        return null;
    }

}
