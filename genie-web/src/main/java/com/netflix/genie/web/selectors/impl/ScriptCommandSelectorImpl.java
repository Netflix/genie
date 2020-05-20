/*
 *
 *  Copyright 2020 Netflix, Inc.
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

import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.scripts.CommandSelectorManagedScript;
import com.netflix.genie.web.scripts.ResourceSelectorScriptResult;
import com.netflix.genie.web.selectors.CommandSelectionContext;
import com.netflix.genie.web.selectors.CommandSelector;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import javax.validation.Valid;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link CommandSelector} which defers the decision to a script provided by the system
 * administrators.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class ScriptCommandSelectorImpl implements CommandSelector {

    static final String SELECT_TIMER_NAME = "genie.selectors.command.script.select.timer";
    private static final String NULL_TAG = "null";
    private static final String NULL_RATIONALE = "Script returned no command, no preference";
    private final CommandSelectorManagedScript commandSelectorManagedScript;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param commandSelectorManagedScript The {@link CommandSelectorManagedScript} instance to use
     * @param registry                     The {@link MeterRegistry} to use
     */
    public ScriptCommandSelectorImpl(
        final CommandSelectorManagedScript commandSelectorManagedScript,
        final MeterRegistry registry
    ) {
        this.commandSelectorManagedScript = commandSelectorManagedScript;
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceSelectionResult<Command> select(
        @Valid final CommandSelectionContext context
    ) throws ResourceSelectionException {
        final long selectStart = System.nanoTime();
        final String jobId = context.getJobId();
        final Set<Command> resources = context.getResources();
        log.debug("Called to select a command from {} for job {}", resources, jobId);
        final Set<Tag> tags = Sets.newHashSet();
        final ResourceSelectionResult.Builder<Command> builder = new ResourceSelectionResult.Builder<>(this.getClass());

        try {
            final ResourceSelectorScriptResult<Command> result
                = this.commandSelectorManagedScript.selectResource(context);
            MetricsUtils.addSuccessTags(tags);

            final Optional<Command> commandOptional = result.getResource();
            if (!commandOptional.isPresent()) {
                final String rationale = result.getRationale().orElse(NULL_RATIONALE);
                log.debug("No command selected due to: {}", rationale);
                tags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_ID, NULL_TAG));
                builder.withSelectionRationale(rationale);
                return builder.build();
            }

            final Command selectedCommand = commandOptional.get();
            tags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_ID, selectedCommand.getId()));
            tags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_NAME, selectedCommand.getMetadata().getName()));

            return builder
                .withSelectionRationale(result.getRationale().orElse(null))
                .withSelectedResource(selectedCommand)
                .build();
        } catch (final Throwable e) {
            final String errorMessage = "Command selection error: " + e.getMessage();
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
}
