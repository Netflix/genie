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
package com.netflix.genie.common.internal.tracing;

import java.util.Map;
import java.util.Optional;

/**
 * This interface exists to provide a shared contract for how trace information is shared between the Genie
 * server job request handling and the agent process launch. Implementations should be shared across server and agent
 * versions for compatibility.
 * <p>
 * Note: This somewhat copies a pattern from <a href="https://github.com/openzipkin/brave">Brave</a> however it is
 * purposely decoupled so as to allow other implementations if necessary.
 *
 * @param <C> The trace context type that should be used for injection and returned from extraction
 * @author tgianos
 * @since 4.0.0
 */
public interface TracePropagator<C> {

    /**
     * Extract the trace context from the supplied set of key value pairs.
     * <p>
     * Implementations should swallow all exceptions as tracing is not critical to the completion of a job on behalf
     * of the user.
     *
     * @param environment Generally this will be the result of {@link System#getenv()}
     * @return A new instance of {@link C} containing the extracted context or {@link Optional#empty()} if no context
     * information was found
     */
    Optional<C> extract(Map<String, String> environment);

    /**
     * Inject the trace context from {@literal U} into the returned set of key value pairs for propagation to Agent.
     * <p>
     * Implementations should swallow all exceptions as tracing is not critical to the completion of a job on behalf
     * of the user.
     *
     * @param traceContext The context for the active unit of work (span in Brave parlance)
     * @return A set of key value pairs that should be propagated to the agent in some manner to be extracted in
     * {@link #extract(Map)}
     */
    Map<String, String> injectForAgent(C traceContext);

    /**
     * Inject the trace context from {@literal U} into the returned set of key value pairs for propagation to job.
     * <p>
     * Implementations should swallow all exceptions as tracing is not critical to the completion of a job on behalf
     * of the user.
     *
     * @param traceContext The context for the active unit of work (span in Brave parlance)
     * @return A set of key value pairs that should be propagated to the job in some manner which can be extracted
     * by downstream systems if they so desire
     */
    Map<String, String> injectForJob(C traceContext);
}
