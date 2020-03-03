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
package com.netflix.genie.web.scripts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import com.netflix.genie.web.properties.CommandSelectorManagedScriptProperties;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;

/**
 * An extension of {@link ManagedScript} which from a set of commands and the original job request will attempt to
 * determine the best command to use for execution.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class CommandSelectorManagedScript extends ManagedScript {

    static final String JOB_REQUEST_BINDING = "jobRequestParameter";
    static final String COMMANDS_BINDING = "commandsParameter";

    /**
     * Constructor.
     *
     * @param scriptManager The {@link ScriptManager} instance to use
     * @param properties    The {@link CommandSelectorManagedScriptProperties} instance to use
     * @param registry      The {@link MeterRegistry} instance to use
     */
    public CommandSelectorManagedScript(
        final ScriptManager scriptManager,
        final CommandSelectorManagedScriptProperties properties,
        final MeterRegistry registry
    ) {
        super(scriptManager, properties, registry);
    }

    /**
     * Given the {@link JobRequest} and an associated set of {@link Command}'s which matched the request criteria
     * invoke the configured script to see if a preferred command is selected based on the current logic.
     *
     * @param commands   The set of {@link Command}'s which should be selected from
     * @param jobRequest The {@link JobRequest} that the commands will be running
     * @return A {@link CommandSelectionResult} instance
     * @throws ResourceSelectionException If an unexpected error occurs during selection
     */
    public CommandSelectionResult selectCommand(
        final Set<Command> commands,
        final JobRequest jobRequest
    ) throws ResourceSelectionException {
        log.debug("Called to attempt to select a command from {} for job {}", commands, jobRequest);

        try {
            final Object evaluationResult = this.evaluateScript(
                ImmutableMap.of(
                    JOB_REQUEST_BINDING, jobRequest,
                    COMMANDS_BINDING, commands
                )
            );
            if (!(evaluationResult instanceof ScriptResult)) {
                throw new ResourceSelectionException(
                    "Command selector evaluation returned invalid type: " + evaluationResult.getClass().getName()
                );
            }
            final ScriptResult result = (ScriptResult) evaluationResult;

            final String selectedId = result.getId().orElse(null);
            final String selectionRationale = result.getRationale().orElse(null);
            if (StringUtils.isNotBlank(selectedId)) {
                return new CommandSelectionResult(
                    commands
                        .stream()
                        .filter(command -> command.getId().equals(selectedId))
                        .findFirst()
                        .orElseThrow(
                            () -> new ResourceSelectionException(
                                "Command with id " + selectedId + " selected but no such command exists in set"
                            )
                        ),
                    selectionRationale
                );
            } else {
                return new CommandSelectionResult(null, selectionRationale);
            }
        } catch (
            final ScriptExecutionException
                | ScriptNotConfiguredException
                | RuntimeException e
        ) {
            throw new ResourceSelectionException(e);
        }
    }

    /**
     * Class to represent a generic response from a script which selects a resource from a set of resources.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Getter
    public static class ScriptResult {
        private final String id;
        private final String rationale;

        /**
         * Constructor.
         *
         * @param id        The {@literal id} of the selected resource if any
         * @param rationale The rationale, if any, for why the given {@literal id} was selected
         */
        @JsonCreator
        public ScriptResult(
            @JsonProperty(value = "id") @Nullable final String id,
            @JsonProperty(value = "rationale") @Nullable final String rationale
        ) {
            this.id = id;
            this.rationale = rationale;
        }

        /**
         * Get the selected resource id if there was one.
         *
         * @return The id wrapped in an {@link Optional} or {@link Optional#empty()}
         */
        public Optional<String> getId() {
            return Optional.ofNullable(this.id);
        }

        /**
         * Get the rationale for the selection decision.
         *
         * @return The rationale wrapped in an {@link Optional} or {@link Optional#empty()}
         */
        public Optional<String> getRationale() {
            return Optional.ofNullable(this.rationale);
        }
    }

    public static class CommandSelectionResult {

        private final Command command;
        private final String rationale;

        CommandSelectionResult(
            @Nullable final Command command,
            @Nullable final String rationale
        ) {
            this.command = command;
            this.rationale = rationale;
        }

        /**
         * Get the {@link Command} this script selected if any.
         *
         * @return The {@link Command} or {@link Optional#empty()}
         */
        public Optional<Command> getCommand() {
            return Optional.ofNullable(this.command);
        }

        /**
         * Get the rationale for the selection of the given command if any.
         *
         * @return The rationale or {@link Optional#empty()}
         */
        public Optional<String> getRationale() {
            return Optional.ofNullable(this.rationale);
        }
    }
}
