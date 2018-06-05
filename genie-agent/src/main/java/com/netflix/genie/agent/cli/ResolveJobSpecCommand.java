/*
 *
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.genie.agent.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.util.GenieObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * Command to request the server to resolve a job request into a job specification.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class ResolveJobSpecCommand implements AgentCommand {

    private final ResolveJobSpecCommandArguments resolveJobSpecCommandArguments;
    private final AgentJobService agentJobService;
    private final JobRequestConverter jobRequestConverter;

    ResolveJobSpecCommand(
        final ResolveJobSpecCommandArguments resolveJobSpecCommandArguments,
        final AgentJobService agentJobService,
        final JobRequestConverter jobRequestConverter
    ) {
        this.resolveJobSpecCommandArguments = resolveJobSpecCommandArguments;
        this.agentJobService = agentJobService;
        this.jobRequestConverter = jobRequestConverter;
    }

    @Override
    public void run() {
        log.info("Resolving job specification");

        final ObjectMapper prettyJsonMapper = GenieObjectMapper.getMapper()
            .copy() // Don't reconfigure the shared mapper
            .enable(SerializationFeature.INDENT_OUTPUT);

        final JobSpecification spec;
        final String jobId = resolveJobSpecCommandArguments.getSpecificationId();
        if (!StringUtils.isBlank(jobId)) {
            // Do a specification lookup if an id is given
            log.info("Looking up specification of job {}", jobId);
            try {
                spec = agentJobService.getJobSpecification(jobId);
            } catch (final JobSpecificationResolutionException e) {
                throw new RuntimeException("Failed to get spec: " + jobId, e);
            }

        } else {
            // Compose a job request from argument
            final AgentJobRequest agentJobRequest;
            try {
                final ArgumentDelegates.JobRequestArguments jobArgs
                    = resolveJobSpecCommandArguments.getJobRequestArguments();
                agentJobRequest = jobRequestConverter.agentJobRequestArgsToDTO(jobArgs);
            } catch (final JobRequestConverter.ConversionException e) {
                throw new RuntimeException("Failed to construct job request from arguments", e);
            }

            // Print request
            if (!resolveJobSpecCommandArguments.isPrintRequestDisabled()) {
                try {
                    System.out.println(prettyJsonMapper.writeValueAsString(agentJobRequest));
                } catch (final JsonProcessingException e) {
                    throw new RuntimeException("Failed to map request to JSON", e);
                }
            }

            // Resolve via service
            try {
                spec = agentJobService.resolveJobSpecificationDryRun(agentJobRequest);
            } catch (final JobSpecificationResolutionException e) {
                throw new RuntimeException("Failed to resolve job specification", e);
            }
        }

        // Translate response to JSON
        final String specJsonString;
        try {
            specJsonString = prettyJsonMapper.writeValueAsString(spec);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException("Failed to map specification to JSON", e);
        }

        // Print specification
        System.out.println(specJsonString);

        // Write specification to file
        final File outputFile = resolveJobSpecCommandArguments.getOutputFile();
        if (outputFile != null) {
            try {
                Files.write(
                    outputFile.toPath(),
                    specJsonString.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE_NEW
                );
            } catch (final IOException e) {
                throw new RuntimeException("Failed to write request to: " + outputFile.getAbsolutePath(), e);
            }
        }
    }

    @Component
    @Parameters(
        commandNames = CommandNames.RESOLVE,
        commandDescription = "Resolve job parameters into a job specification via server")
    @Getter
    static class ResolveJobSpecCommandArguments implements AgentCommandArguments {

        @ParametersDelegate
        private final ArgumentDelegates.ServerArguments serverArguments;

        @ParametersDelegate
        private final ArgumentDelegates.JobRequestArguments jobRequestArguments;

        @Parameter(
            names = {"--spec-id"},
            description = "Lookup an existing specification rather than resolving (ignores other job arguments)"
        )
        private String specificationId;

        @Parameter(
            names = {"--output-file"},
            description = "Output file for specification (in JSON form)",
            converter = ArgumentConverters.FileConverter.class
        )
        private File outputFile;

        @Parameter(
            names = {"--no-request"},
            description = "Do not print the request to console"
        )
        private boolean printRequestDisabled;

        ResolveJobSpecCommandArguments(
            final ArgumentDelegates.ServerArguments serverArguments,
            final ArgumentDelegates.JobRequestArguments jobRequestArguments
        ) {
            this.serverArguments = serverArguments;
            this.jobRequestArguments = jobRequestArguments;
        }

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return ResolveJobSpecCommand.class;
        }
    }
}
