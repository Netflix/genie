/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.selectors.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.scripts.AgentLauncherSelectorManagedScript;
import com.netflix.genie.web.scripts.ResourceSelectorScriptResult;
import com.netflix.genie.web.selectors.AgentLauncherSelectionContext;
import com.netflix.genie.web.selectors.AgentLauncherSelector;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import javax.validation.Valid;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of the {@link AgentLauncherSelector} interface which uses user-provided script to make decisions
 * based on the list of agent launchers and the job request supplied.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ScriptAgentLauncherSelectorImpl implements AgentLauncherSelector {

    static final String SELECT_TIMER_NAME = "genie.jobs.agentLauncher.selectors.script.select.timer";
    private static final String NULL_TAG = "null";
    private static final String NULL_RATIONALE = "Script returned null, no preference";

    private final AgentLauncherSelectorManagedScript agentLauncherSelectorManagedScript;
    private final Set<AgentLauncher> agentLaunchers;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param agentLauncherSelectorManagedScript the agent launcher selector script
     * @param agentLaunchers                     the available agent launchers
     * @param registry                           the metrics registry
     */
    public ScriptAgentLauncherSelectorImpl(
        final AgentLauncherSelectorManagedScript agentLauncherSelectorManagedScript,
        final Collection<AgentLauncher> agentLaunchers,
        final MeterRegistry registry
    ) {
        this.agentLauncherSelectorManagedScript = agentLauncherSelectorManagedScript;
        this.agentLaunchers = ImmutableSet.copyOf(agentLaunchers);
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceSelectionResult<AgentLauncher> select(
        @Valid final AgentLauncherSelectionContext context
    ) throws ResourceSelectionException {
        final long selectStart = System.nanoTime();
        final String jobId = context.getJobId();
        final Set<AgentLauncher> resources = context.getAgentLaunchers();
        log.debug("Called to select agent launcher from {} for job {}", resources, jobId);
        final Set<Tag> tags = Sets.newHashSet();
        final ResourceSelectionResult.Builder<AgentLauncher> builder =
            new ResourceSelectionResult.Builder<>(this.getClass());

        try {
            final ResourceSelectorScriptResult<AgentLauncher> result
                = this.agentLauncherSelectorManagedScript.selectResource(context);
            MetricsUtils.addSuccessTags(tags);

            final Optional<AgentLauncher> agentLauncherOptional = result.getResource();
            if (!agentLauncherOptional.isPresent()) {
                final String rationale = result.getRationale().orElse(NULL_RATIONALE);
                log.debug("No agent launcher selected due to: {}", rationale);
                tags.add(Tag.of(MetricsConstants.TagKeys.AGENT_LAUNCHER_CLASS, NULL_TAG));
                builder.withSelectionRationale(rationale);
                return builder.build();
            }

            final AgentLauncher selectedAgentLauncher = agentLauncherOptional.get();
            tags.add(
                Tag.of(MetricsConstants.TagKeys.AGENT_LAUNCHER_CLASS, selectedAgentLauncher.getClass().getSimpleName())
            );

            return builder
                .withSelectionRationale(result.getRationale().orElse(null))
                .withSelectedResource(selectedAgentLauncher)
                .build();
        } catch (final Throwable e) {
            final String errorMessage = "Agent launcher selection error: " + e.getMessage();
            log.error(errorMessage, e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            if (e instanceof ResourceSelectionException) {
                throw e;
            } else {
                throw new ResourceSelectionException(e);
            }
        } finally {
            this.registry
                .timer(SELECT_TIMER_NAME, tags)
                .record(System.nanoTime() - selectStart, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public Collection<AgentLauncher> getAgentLaunchers() {
        return agentLaunchers;
    }
}
