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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import com.netflix.genie.web.properties.CommandSelectorManagedScriptProperties;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AccessLevel;
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

    static final String JOB_REQUEST_BINDING = "jobRequest";
    static final String COMMANDS_BINDING = "commands";

    /**
     * Constructor.
     *
     * @param scriptManager The {@link ScriptManager} instance to use
     * @param properties    The {@link CommandSelectorManagedScriptProperties} instance to use
     * @param mapper        The {@link ObjectMapper} instance to use
     * @param registry      The {@link MeterRegistry} instance to use
     */
    public CommandSelectorManagedScript(
        final ScriptManager scriptManager,
        final CommandSelectorManagedScriptProperties properties,
        final ObjectMapper mapper,
        final MeterRegistry registry
    ) {
        super(scriptManager, properties, mapper, registry);
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
            final ScriptResult result = this.getMapper()
                .readValue(
                    (String) this.evaluateScript(
                        ImmutableMap.of(
                            JOB_REQUEST_BINDING, jobRequest,
                            COMMANDS_BINDING, commands
                        )
                    ),
                    ScriptResult.class
                );
            if (StringUtils.isNotBlank(result.commandId)) {
                return new CommandSelectionResult(
                    commands
                        .stream()
                        .filter(command -> command.getId().equals(result.commandId))
                        .findFirst()
                        .orElseThrow(
                            () -> new ResourceSelectionException(
                                "Command with id " + result.commandId + " selected but no such command exists in set"
                            )
                        ),
                    result.rationale
                );
            } else {
                return new CommandSelectionResult(null, result.rationale);
            }
        } catch (
            final ScriptExecutionException
                | ScriptNotConfiguredException
                | JsonProcessingException
                | RuntimeException e
        ) {
            throw new ResourceSelectionException(e);
        }
    }

    @Getter(AccessLevel.PACKAGE)
    static class ScriptResult {
        private final String commandId;
        private final String rationale;

        @JsonCreator
        ScriptResult(
            @JsonProperty(value = "commandId") @Nullable final String commandId,
            @JsonProperty(value = "rationale") @Nullable final String rationale
        ) {
            this.commandId = commandId;
            this.rationale = rationale;
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
