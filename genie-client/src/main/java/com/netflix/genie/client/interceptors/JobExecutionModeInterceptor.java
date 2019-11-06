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
package com.netflix.genie.client.interceptors;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * A temporary interceptor to be used during migration from V3 embedded execution to V4 agent execution. Allows clients
 * to force one way or another if desired or if neither just rely on the default behavior of the server.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class JobExecutionModeInterceptor implements Interceptor {

    /**
     * If the user wants to force all their jobs to execute with the V3 embedded code they should set this property
     * to true.
     */
    public static final String EXECUTE_EMBEDDED_PROPERTY = "genie.client.jobs.execute.embedded";

    /**
     * If the user wants to force all their jobs to execute with the V4 agent they should set this property to true.
     */
    public static final String EXECUTE_AGENT_PROPERTY = "genie.client.jobs.execute.agent";

    // TODO: Might be nice to put these somewhere public/shared in OSS
    @VisibleForTesting
    static final String FORCE_AGENT_EXECUTION_HEADER_NAME = "genie-force-agent-execution";
    @VisibleForTesting
    static final String FORCE_EMBEDDED_EXECUTION_HEADER_NAME = "genie-force-embedded-execution";
    @VisibleForTesting
    static final String TRUE_STRING = Boolean.TRUE.toString();
    @VisibleForTesting
    static final String JOBS_API = "/api/v3/jobs";

    private static final String POST_METHOD = "post";

    private final Supplier<Boolean> executeWithEmbeddedSupplier;
    private final Supplier<Boolean> executeWithAgentSupplier;

    /**
     * Constructor.
     *
     * @param executeWithEmbeddedSupplier A {@link Supplier} function that should return {@literal true} if the
     *                                    application wants to force Genie to execute the job with the old embedded
     *                                    method
     * @param executeWithAgentSupplier    A {@link Supplier} function that should return {@literal true} if the
     *                                    application wants to force Genie to execute the job with the new Agent
     *                                    execution mode
     */
    public JobExecutionModeInterceptor(
        final Supplier<Boolean> executeWithEmbeddedSupplier,
        final Supplier<Boolean> executeWithAgentSupplier
    ) {
        this.executeWithAgentSupplier = executeWithAgentSupplier;
        this.executeWithEmbeddedSupplier = executeWithEmbeddedSupplier;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Response intercept(final Chain chain) throws IOException {
        final Request request = chain.request();
        // Only care about new job submissions don't set these headers on every request...
        if (request.method().equalsIgnoreCase(POST_METHOD) && request.url().encodedPath().equalsIgnoreCase(JOBS_API)) {
            if (this.executeWithEmbeddedSupplier.get()) {
                log.debug(
                    "Adding {} = true header based on application settings at time of request",
                    FORCE_EMBEDDED_EXECUTION_HEADER_NAME
                );
                return chain.proceed(
                    request
                        .newBuilder()
                        .addHeader(FORCE_EMBEDDED_EXECUTION_HEADER_NAME, TRUE_STRING)
                        .build()
                );
            } else if (this.executeWithAgentSupplier.get()) {
                log.debug(
                    "Adding {} = true header based on application settings at time of request",
                    FORCE_AGENT_EXECUTION_HEADER_NAME
                );
                return chain.proceed(
                    request
                        .newBuilder()
                        .addHeader(FORCE_AGENT_EXECUTION_HEADER_NAME, TRUE_STRING)
                        .build()
                );
            } else {
                log.debug(
                    "No job execution mode forced by application settings at time of request. Not modifying request"
                );
            }
        }

        return chain.proceed(request);
    }
}
