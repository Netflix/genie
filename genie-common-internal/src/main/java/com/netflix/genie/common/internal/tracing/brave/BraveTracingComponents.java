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

import brave.Tracer;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Container DTO class for <a href="https://github.com/openzipkin/brave">Brave</a> based components for tracing in
 * Genie server and agent.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Data
@AllArgsConstructor
public class BraveTracingComponents {
    private final Tracer tracer;
    private final BraveTracePropagator tracePropagator;
    private final BraveTracingCleanup tracingCleaner;
    private final BraveTagAdapter tagAdapter;
}
