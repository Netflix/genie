/*
 *
 *  Copyright 2026 Netflix, Inc.
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
package com.netflix.genie.web.util;

import com.netflix.genie.common.internal.dtos.ExecutionEnvironment;
import com.netflix.genie.common.internal.exceptions.unchecked.UnsafeResourceUriException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Set;

/**
 * Validates that setupFile/config/dependency URIs supplied through Genie's REST API only reference
 * network-fetchable resources, never a scheme that resolves against the local filesystem of whatever host
 * ends up running the job.
 *
 * <p>These fields are ordinary strings, validated elsewhere only for length and non-blankness. A
 * {@link com.netflix.genie.web.agent.launchers.AgentLauncher AgentLauncher}-managed Genie Agent (local or
 * Titus) resolves them on infrastructure the API caller does not control, so a {@code file:} URI here
 * would let any caller read arbitrary files off that infrastructure.
 *
 * <p>Genie's CLI has a separate, legitimate use of {@code file:} URIs for local, interactive job execution
 * ({@code genie-agent/.../cli/ArgumentConverters.java}), where the agent resolving the URI runs on the same
 * machine as whoever specified it. That flow never goes through this REST API and is unaffected by this
 * validator.
 *
 * @author roridemonslayer
 * @since 4.4.0
 */
public final class ResourceUriValidator {

    private static final Set<String> ALLOWED_SCHEMES = Set.of("http", "https", "s3");

    private ResourceUriValidator() {
    }

    /**
     * Validate the setupFile, configs, and dependencies of a resource's {@link ExecutionEnvironment}.
     *
     * @param resources the execution environment to validate
     * @throws UnsafeResourceUriException if any URI uses a disallowed scheme
     */
    public static void validate(final ExecutionEnvironment resources) {
        resources.getSetupFile().ifPresent(ResourceUriValidator::validateUri);
        validate(resources.getConfigs());
        validate(resources.getDependencies());
    }

    /**
     * Validate a bare set of config or dependency URI strings, for callers (e.g. the configs/dependencies
     * sub-resource endpoints) that don't have a full {@link ExecutionEnvironment} to work with.
     *
     * @param uris the URI strings to validate
     * @throws UnsafeResourceUriException if any URI uses a disallowed scheme
     */
    public static void validate(final Set<String> uris) {
        for (final String uri : uris) {
            validateUri(uri);
        }
    }

    private static void validateUri(final String uriString) {
        final URI uri;
        try {
            uri = new URI(uriString);
        } catch (final URISyntaxException e) {
            throw new UnsafeResourceUriException("Invalid resource URI: '" + uriString + "'");
        }
        final String scheme = uri.getScheme();
        if (scheme == null || !ALLOWED_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT))) {
            throw new UnsafeResourceUriException(
                "Unsupported resource URI scheme in '" + uriString + "'. Allowed schemes: " + ALLOWED_SCHEMES
            );
        }
    }
}
