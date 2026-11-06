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
package com.netflix.genie.common.internal.exceptions.unchecked;

/**
 * Thrown when a setupFile, config, or dependency URI supplied through the REST API uses a scheme that
 * isn't safe to let a server-managed Genie Agent resolve on the caller's behalf (e.g. {@code file:}).
 *
 * @author roridemonslayer
 * @since 4.4.0
 */
public class UnsafeResourceUriException extends GenieRuntimeException {
    /**
     * Constructor.
     *
     * @param message The detail message
     */
    public UnsafeResourceUriException(final String message) {
        super(message);
    }
}
