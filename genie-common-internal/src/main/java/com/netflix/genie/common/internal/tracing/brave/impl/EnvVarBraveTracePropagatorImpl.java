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
package com.netflix.genie.common.internal.tracing.brave.impl;

import brave.propagation.TraceContext;
import com.netflix.genie.common.internal.tracing.TracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Implementation of {@link TracePropagator} based on <a href="https://github.com/openzipkin/brave">Brave</a> and
 * <a href="https://github.com/openzipkin/b3-propagation">B3 Propagation</a>. This particular implementation leverages
 * environment variables to pass context between processes.
 * <p>
 * Note: This current implementation kind of breaks the contract as we don't propagate all the expected headers.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class EnvVarBraveTracePropagatorImpl implements BraveTracePropagator {

    /**
     * The key for the active trace id low set of bits.
     *
     * @see TraceContext.Builder#traceId(long)
     */
    static final String GENIE_AGENT_B3_TRACE_ID_LOW_KEY = "GENIE_AGENT_B3_TRACE_ID_LOW";

    /**
     * The key for the active trace id high set of bits.
     *
     * @see TraceContext.Builder#traceIdHigh(long)
     */
    static final String GENIE_AGENT_B3_TRACE_ID_HIGH_KEY = "GENIE_AGENT_B3_TRACE_ID_HIGH";

    /**
     * The key for the span id.
     *
     * @see TraceContext.Builder#spanId(long)
     */
    static final String GENIE_AGENT_B3_SPAN_ID_KEY = "GENIE_AGENT_B3_SPAN_ID";

    /**
     * The key for the parent span.
     *
     * @see TraceContext.Builder#parentId(long)
     */
    static final String GENIE_AGENT_B3_PARENT_SPAN_ID_KEY = "GENIE_AGENT_B3_PARENT_SPAN_ID";

    /**
     * The key for whether or not spans should be sampled.
     *
     * @see TraceContext.Builder#sampled(Boolean)
     */
    static final String GENIE_AGENT_B3_SAMPLED_KEY = "GENIE_AGENT_B3_SAMPLED";

    /**
     * The key for the active trace id low set of bits.
     *
     * @see TraceContext.Builder#traceId(long)
     */
    static final String GENIE_JOB_B3_TRACE_ID_LOW_KEY = "GENIE_B3_TRACE_ID_LOW";

    /**
     * The key for the active trace id high set of bits.
     *
     * @see TraceContext.Builder#traceIdHigh(long)
     */
    static final String GENIE_JOB_B3_TRACE_ID_HIGH_KEY = "GENIE_B3_TRACE_ID_HIGH";

    /**
     * The key for the span id.
     *
     * @see TraceContext.Builder#spanId(long)
     */
    static final String GENIE_JOB_B3_SPAN_ID_KEY = "GENIE_B3_SPAN_ID";

    /**
     * The key for the parent span of the job.
     *
     * @see TraceContext.Builder#parentId(long)
     */
    static final String GENIE_JOB_B3_PARENT_SPAN_ID_KEY = "GENIE_B3_PARENT_SPAN_ID";

    /**
     * The key for whether or not spans should be sampled.
     *
     * @see TraceContext.Builder#sampled(Boolean)
     */
    static final String GENIE_JOB_B3_SAMPLED_KEY = "GENIE_B3_SAMPLED";

    private static final Logger LOG = LoggerFactory.getLogger(EnvVarBraveTracePropagatorImpl.class);
    private static final long NO_TRACE_ID_HIGH = 0L;

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<TraceContext> extract(final Map<String, String> environment) {
        /*
         * While use cases for Tracing are potentially important they are not as important as actually running the users
         * job. For this reason if an exception happens during Trace context creation it will be swallowed and the
         * system will have to move on either with a new trace completely or some other mechanism to be determined
         * upstream from this method.
         */
        try {
            final String traceIdLow = environment.get(GENIE_AGENT_B3_TRACE_ID_LOW_KEY);
            if (StringUtils.isBlank(traceIdLow)) {
                LOG.debug("No trace id low found in the supplied set of key value pairs. Can't extract trace context");
                return Optional.empty();
            }
            final String spanId = environment.get(GENIE_AGENT_B3_SPAN_ID_KEY);
            if (StringUtils.isBlank(spanId)) {
                LOG.debug(
                    "No span id found in the supplied set of key value pairs. Can't extract trace context"
                );
                return Optional.empty();
            }

            final TraceContext.Builder builder = TraceContext.newBuilder();
            builder.traceId(Long.parseLong(traceIdLow));
            builder.spanId(Long.parseLong(spanId));

            final String traceIdHigh = environment.get(GENIE_AGENT_B3_TRACE_ID_HIGH_KEY);
            if (StringUtils.isNotBlank(traceIdHigh)) {
                builder.traceIdHigh(Long.parseLong(traceIdHigh));
            }

            final String parentSpanId = environment.get(GENIE_AGENT_B3_PARENT_SPAN_ID_KEY);
            if (StringUtils.isNotBlank(parentSpanId)) {
                builder.parentId(Long.parseLong(parentSpanId));
            }

            final String sampled = environment.get(GENIE_AGENT_B3_SAMPLED_KEY);
            if (StringUtils.isNotBlank(sampled)) {
                builder.sampled(Boolean.parseBoolean(sampled));
            }

            LOG.debug(
                "Extracted trace context: "
                    + "Trace Id Low = {}, "
                    + "Trace Id High = {}, "
                    + "Parent Span Id = {}, "
                    + "New Span Id = {}, "
                    + "Sampled = {}",
                traceIdLow,
                traceIdHigh,
                parentSpanId,
                spanId,
                sampled
            );

            return Optional.of(builder.build());
        } catch (final Throwable t) {
            LOG.warn(
                "Unable to extract trace context from supplied key value pairs due to exception: {}",
                t.getMessage(),
                t
            );
            return Optional.empty();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> injectForAgent(final TraceContext traceContext) {
        return this.inject(traceContext, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> injectForJob(final TraceContext traceContext) {
        return this.inject(traceContext, false);
    }

    private Map<String, String> inject(final TraceContext traceContext, final boolean isForAgent) {
        try {
            final Map<String, String> propagationContext = new HashMap<>();
            propagationContext.put(
                isForAgent ? GENIE_AGENT_B3_TRACE_ID_LOW_KEY : GENIE_JOB_B3_TRACE_ID_LOW_KEY,
                Long.toString(traceContext.traceId())
            );
            propagationContext.put(
                isForAgent ? GENIE_AGENT_B3_SPAN_ID_KEY : GENIE_JOB_B3_SPAN_ID_KEY,
                Long.toString(traceContext.spanId())
            );
            propagationContext.put(
                isForAgent ? GENIE_AGENT_B3_SAMPLED_KEY : GENIE_JOB_B3_SAMPLED_KEY,
                traceContext.sampled().toString()
            );

            // only propagate high bits of trace id if they're non-zero
            final long traceIdHigh = traceContext.traceIdHigh();
            if (traceIdHigh != NO_TRACE_ID_HIGH) {
                propagationContext.put(
                    isForAgent ? GENIE_AGENT_B3_TRACE_ID_HIGH_KEY : GENIE_JOB_B3_TRACE_ID_HIGH_KEY,
                    Long.toString(traceIdHigh)
                );
            }

            final Long parentSpanId = traceContext.parentId();
            if (parentSpanId != null) {
                propagationContext.put(
                    isForAgent ? GENIE_AGENT_B3_PARENT_SPAN_ID_KEY : GENIE_JOB_B3_PARENT_SPAN_ID_KEY,
                    Long.toString(parentSpanId)
                );
            }

            return propagationContext;
        } catch (final Throwable t) {
            LOG.warn(
                "Unable to inject trace context for propagation to {} due to {}",
                isForAgent ? "agent" : "job",
                t.getMessage(),
                t
            );
            // Since we don't know what has been injected and what hasn't for now just punt and return
            // empty so that downstream starts fresh
            return new HashMap<>();
        }
    }
}
