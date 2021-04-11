/*
 *
 *  Copyright 2021 Netflix, Inc.
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
package com.netflix.genie.common.internal.tracing.brave;

import zipkin2.reporter.AsyncReporter;

import java.util.Set;

/**
 * Any cleanup needed at program shutdown for <a href="https://github.com/openzipkin/brave">Brave</a> instrumentation.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class BraveTracingCleanup {

    private final Set<AsyncReporter<?>> reporters;

    /**
     * Constructor.
     *
     * @param reporters Any {@link AsyncReporter} instance configured for the system
     */
    public BraveTracingCleanup(final Set<AsyncReporter<?>> reporters) {
        this.reporters = reporters;
    }

    /**
     * Should be called at the end of the program to perform any necessary cleanup that native Brave components don't
     * already do. Example: flushing asynchronous reporters so that spans are more guaranteed to be reported than if
     * this wasn't explicitly called and a timeout happened.
     */
    public void cleanup() {
        this.reporters.forEach(AsyncReporter::flush);
    }
}
