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
package com.netflix.genie.web.apis.rest.v3.controllers;

import com.google.common.collect.Sets;
import org.springframework.test.context.ActiveProfilesResolver;

import java.util.Set;

/**
 * A class to switch the active profiles for integration tests based on environment variables.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class IntegrationTestActiveProfilesResolver implements ActiveProfilesResolver {

    private static final String DB_SELECTOR_ENV_VARIABLE_NAME = "INTEGRATION_TEST_DB";
    private static final String MYSQL = "mysql";
    private static final String POSTGRESQL = "postgresql";
    private static final String H2 = "h2";
    private final Set<String> knownDatabaseProfiles = Sets.newHashSet(
        MYSQL,
        POSTGRESQL,
        H2
    );

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] resolve(final Class<?> testClass) {
        final String integrationTestDatabase = System.getProperty(DB_SELECTOR_ENV_VARIABLE_NAME, H2);

        if (!this.knownDatabaseProfiles.contains(integrationTestDatabase)) {
            throw new IllegalStateException("Unknown database profile: " + integrationTestDatabase);
        }

        return new String[]{
            "integration",
            "db",
            "db-" + integrationTestDatabase,
        };
    }
}
