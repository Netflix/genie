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
package com.netflix.genie.web.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Properties related to number of active jobs per user.
 *
 * @author mprimi
 * @since 3.1.0
 */
@ConfigurationProperties(prefix = JobsActiveLimitProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class JobsActiveLimitProperties implements EnvironmentAware {

    /**
     * The property prefix for job user limiting.
     */
    public static final String PROPERTY_PREFIX = "genie.jobs.active-limit";

    /**
     * The property key for whether this feature is enabled or not.
     */
    public static final String ENABLED_PROPERTY = PROPERTY_PREFIX + ".enabled";

    /**
     * The property key prefix for per-user limit.
     */
    public static final String USER_LIMIT_OVERRIDE_PROPERTY_PREFIX = PROPERTY_PREFIX + ".overrides.";

    /**
     * Default value for active user job limit enabled.
     */
    public static final boolean DEFAULT_ENABLED = false;

    /**
     * Default value for active user job limit count.
     */
    public static final int DEFAULT_COUNT = 100;

    private boolean enabled = DEFAULT_ENABLED;

    @Min(value = 1)
    private int count = DEFAULT_COUNT;
    private AtomicReference<Environment> environment = new AtomicReference<>();

    /**
     * Get the maximum number of jobs a user is allowed to run concurrently.
     * Checks whether the user has a special limit associated, and if not it returns the global default.
     *
     * @param user the user name
     * @return the maximum number of jobs
     */
    public int getUserLimit(final String user) {
        final Environment env = this.environment.get();
        if (env != null) {
            return env.getProperty(
                USER_LIMIT_OVERRIDE_PROPERTY_PREFIX + user,
                Integer.class,
                this.count
            );
        }
        return this.count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEnvironment(final Environment environment) {
        this.environment.set(environment);
    }
}
