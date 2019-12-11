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
package com.netflix.genie.web.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import com.netflix.genie.web.scripts.ExecutionModeFilterScript;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.core.env.Environment;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * This service inspects an incoming job requests and makes a decision on whether the job should be executed via the
 * "legacy" embedded mode (i.e. Genie V3) or the new V4 Agent execution.
 * The service is a temporary utility to ease transition from V3 to V4 and will eventually be removed.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class JobExecutionModeSelector {


    /**
     * Functional interface for methods of this class that check the incoming job request and may decide for one or the
     * other execution mode.
     */
    @FunctionalInterface
    private interface CheckRequestFunction {
        Optional<Boolean> check(JobRequest jobRequest, HttpServletRequest httpServletRequest);
    }

    private static final String PROPERTY_PREFIX = "genie.jobs.agent-execution.";
    private static final Boolean DEFAULT_EXECUTE_WITH_AGENT = false;
    private static final String FORCE_AGENT_EXECUTION_HEADER_NAME = "genie-force-agent-execution";
    private static final String FORCE_EMBEDDED_EXECUTION_HEADER_NAME = "genie-force-embedded-execution";
    private static final String GLOBAL_AGENT_OVERRIDE_PROPERTY = PROPERTY_PREFIX + "force-agent";
    private static final String GLOBAL_EMBEDDED_OVERRIDE_PROPERTY = PROPERTY_PREFIX + "force-embedded";
    private static final String AGENT_PROBABILITY_PROPERTY = PROPERTY_PREFIX + "agent-probability";
    private static final String METRIC_NAME = "genie.jobs.executionMode.counter";
    private static final String OUTCOME_METRIC_TAG_NAME = "executeWithAgent";
    private static final String CHECK_METRIC_TAG_NAME = "decidingCheck";
    private static final String DEFAULT = "default";

    /**
     * Property controlling default/fallback execution mode if none of the checks decides otherwise.
     */
    @VisibleForTesting
    public static final String DEFAULT_EXECUTE_WITH_AGENT_PROPERTY = PROPERTY_PREFIX + DEFAULT;

    private final Environment environment;
    private final Random random;
    @Nullable
    private final ExecutionModeFilterScript executionModeFilterScript;
    private final MeterRegistry meterRegistry;
    private final List<Pair<String, CheckRequestFunction>> checks = ImmutableList.of(
        // Global override that applies to all incoming jobs
        new ImmutablePair<>("global-override", this::checkGlobalOverride),
        // Job has HTTP header explicitly requesting a given execution mode
        new ImmutablePair<>("header-override", this::checkForcingHeader),
        // Use plug-in script to designate specific jobs based on their type or attributes
        new ImmutablePair<>("filter-script", this::checkFilterScript),
        // Job execution via agent for a fixed percentage of jobs, chosen randomly
        new ImmutablePair<>("percentage", this::checkProbability)
    );

    /**
     * Constructor.
     *
     * @param environment               the environment
     * @param meterRegistry             the meter registry
     * @param executionModeFilterScript the execution mode selector script
     */
    public JobExecutionModeSelector(
        final Environment environment,
        final MeterRegistry meterRegistry,
        @Nullable final ExecutionModeFilterScript executionModeFilterScript
    ) {
        this(new Random(), environment, meterRegistry, executionModeFilterScript);
    }

    /**
     * Constructor with random generator used for tests.
     *
     * @param random                    the random number generator
     * @param environment               the environment
     * @param meterRegistry             the meter registry
     * @param executionModeFilterScript the execution mode selector script, if one is configured, or null otherwise
     */
    @VisibleForTesting
    JobExecutionModeSelector(
        final Random random,
        final Environment environment,
        final MeterRegistry meterRegistry,
        @Nullable final ExecutionModeFilterScript executionModeFilterScript
    ) {
        this.environment = environment;
        this.random = random;
        this.meterRegistry = meterRegistry;
        this.executionModeFilterScript = executionModeFilterScript;
    }

    /**
     * Inspect the incoming request and decide whether to execute using the new Agent-based execution mode.
     *
     * @param jobRequest         the job request
     * @param httpServletRequest the raw http request
     * @return true if this job should be executed using the agent
     */
    public boolean executeWithAgent(final JobRequest jobRequest, final HttpServletRequest httpServletRequest) {
        // Perform checks in order
        for (final Pair<String, CheckRequestFunction> nameFunctionCheckPair : this.checks) {
            final String checkName = nameFunctionCheckPair.getKey();
            final CheckRequestFunction checkFunction = nameFunctionCheckPair.getValue();
            final Optional<Boolean> checkOptionalOutcome = checkFunction.check(jobRequest, httpServletRequest);

            // Stop at the first check that returns a non-empty Optional
            if (checkOptionalOutcome.isPresent()) {
                final boolean executeWithAgent = checkOptionalOutcome.get();
                this.publishMetric(executeWithAgent, checkName);
                return executeWithAgent;
            }
        }

        // If none of checks made a decision, fall back to default
        final boolean executeWithAgent = this.environment.getProperty(
            DEFAULT_EXECUTE_WITH_AGENT_PROPERTY,
            Boolean.class,
            DEFAULT_EXECUTE_WITH_AGENT
        );

        this.publishMetric(executeWithAgent, DEFAULT);
        return executeWithAgent;
    }

    /*
     * Check 2 dynamic properties that may force all jobs to execute in agent or legacy mode.
     * If neither properties are set, this check expresses no preference.
     */
    private Optional<Boolean> checkGlobalOverride(
        final JobRequest jobRequest,
        final HttpServletRequest httpServletRequest
    ) {
        if (this.environment.getProperty(GLOBAL_AGENT_OVERRIDE_PROPERTY, Boolean.class, false)) {
            log.debug("Forcing agent execution globally");
            return Optional.of(true);
        } else if (this.environment.getProperty(GLOBAL_EMBEDDED_OVERRIDE_PROPERTY, Boolean.class, false)) {
            log.debug("Forcing embedded execution globally");
            return Optional.of(false);
        } else {
            return Optional.empty();
        }
    }

    /*
     * Check HTTP request headers and honor explicit client request to execute in agent or embedded mode.
     * If neither headers are present, this check expresses no preference.
     */
    private Optional<Boolean> checkForcingHeader(
        final JobRequest jobRequest,
        final HttpServletRequest httpServletRequest
    ) {
        if (Boolean.parseBoolean(httpServletRequest.getHeader(FORCE_AGENT_EXECUTION_HEADER_NAME))) {
            log.debug("Forcing agent execution as per request header");
            return Optional.of(true);
        } else if (Boolean.parseBoolean(httpServletRequest.getHeader(FORCE_EMBEDDED_EXECUTION_HEADER_NAME))) {
            log.debug("Forcing embedded execution as per request header");
            return Optional.of(false);
        } else {
            return Optional.empty();
        }
    }

    /*
     * If the property is defined that designates a given percentage of jobs to be executed in agent mode, then roll
     * a dice and assign an execution mode for the job based on that percentage.
     * If the property is not set, this check expresses no preference.
     */
    private Optional<Boolean> checkProbability(
        final JobRequest jobRequest,
        final HttpServletRequest httpServletRequest
    ) {
        final String agentExecutionProbabilityString = this.environment.getProperty(AGENT_PROBABILITY_PROPERTY);

        if (StringUtils.isBlank(agentExecutionProbabilityString)) {
            return Optional.empty();
        }

        final float agentExecutionProbability = Float.parseFloat(agentExecutionProbabilityString);
        if (agentExecutionProbability < 0.0 || agentExecutionProbability > 1.0) {
            log.info("Invalid probability: {}", agentExecutionProbability);
            return Optional.empty();
        }

        if (this.random.nextFloat() < agentExecutionProbability) {
            log.debug("Job randomly selected for agent execution");
            return Optional.of(true);
        } else {
            log.debug("Job randomly selected for embedded execution");
            return Optional.of(false);
        }
    }

    /*
     * If the property is defined that designates a given percentage of jobs to be executed in agent mode, then roll
     * a dice and assign an execution mode for the job based on that percentage.
     * If the property is not set, this check expresses no preference.
     */
    private Optional<Boolean> checkFilterScript(
        final JobRequest jobRequest,
        final HttpServletRequest httpServletRequest
    ) {
        if (this.executionModeFilterScript != null) {
            try {
                return this.executionModeFilterScript.forceAgentExecution(jobRequest);
            } catch (ScriptExecutionException | ScriptNotConfiguredException e) {
                log.error("Script filter error: " + e.getMessage(), e);
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private void publishMetric(final boolean executeWithAgent, final String checkName) {
        log.info(
            "Job designated for {} execution by check: {}",
            executeWithAgent ? "agent" : "embedded",
            checkName
        );
        this.meterRegistry.counter(
            METRIC_NAME,
            Sets.newHashSet(
                Tag.of(OUTCOME_METRIC_TAG_NAME, String.valueOf(executeWithAgent)),
                Tag.of(CHECK_METRIC_TAG_NAME, checkName)
            )
        ).increment();
    }
}
