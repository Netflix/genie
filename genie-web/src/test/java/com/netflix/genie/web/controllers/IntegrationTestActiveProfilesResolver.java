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
package com.netflix.genie.web.controllers;

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
    private static final String HSQL_MEM = "hsql-mem";
    private final Set<String> knownDatabaseProfiles = Sets.newHashSet(
        MYSQL,
        POSTGRESQL,
        HSQL_MEM
    );

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] resolve(final Class<?> testClass) {
        final String integrationTestDatabase = System.getProperty(DB_SELECTOR_ENV_VARIABLE_NAME, MYSQL);

        if (!knownDatabaseProfiles.contains(integrationTestDatabase)) {
            throw new IllegalStateException("Unknown database profile: " + integrationTestDatabase);
        }

        return new String[] {
            "integration",
            "db",
            "db-" + integrationTestDatabase,
        };
    }
}
