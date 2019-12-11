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
package com.netflix.genie.web.scripts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import com.netflix.genie.web.properties.ExecutionModeFilterScriptProperties;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;

/**
 * Implementation of {@link ManagedScript} that delegates selection of a job's execution mode to a script,
 * thus allowing fine-grained blacklisting/whitelisting when it comes to execution mode.
 * See also: {@link com.netflix.genie.web.util.JobExecutionModeSelector}.
 * <p>
 * The contract between the script and the Java code is that the script will be supplied global variables
 * {@code jobRequest} which will be JSON strings representing job request that kicked off this evaluation.
 * The code expects the script to either return the true (to force agent execution), false (to force legacy/embedded
 * execution) or null (for no preference).
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ExecutionModeFilterScript extends ManagedScript {
    private static final String JOB_REQUEST_BINDING = "jobRequest";

    /**
     * Constructor.
     *
     * @param scriptManager script manager
     * @param properties    script properties
     * @param mapper        object mapper
     * @param registry      meter registry
     */
    public ExecutionModeFilterScript(
        final ScriptManager scriptManager,
        final ExecutionModeFilterScriptProperties properties,
        final ObjectMapper mapper,
        final MeterRegistry registry
    ) {
        super(scriptManager, properties, mapper, registry);
    }

    /**
     * Evaluate the script and return true if this job should be forced to execute via agent, false if it should be
     * forced to execute in embedded mode, null if the script decides not explicitly flag this job for one or the other
     * execution mode.
     *
     * @param jobRequest the job request
     * @return An optional boolean value
     * @throws ScriptNotConfiguredException if the script is notyet successfully loaded and compiled
     * @throws ScriptExecutionException     if the script evaluation produces an error
     */
    public Optional<Boolean> forceAgentExecution(
        final JobRequest jobRequest
    ) throws ScriptNotConfiguredException, ScriptExecutionException {
        final Map<String, Object> scriptParameters = ImmutableMap.of(JOB_REQUEST_BINDING, jobRequest);

        final Object scriptOutput = this.evaluateScript(scriptParameters);
        log.debug("Execution mode selector returned: {} for job request: {}", scriptOutput, jobRequest);

        if (scriptOutput == null) {
            return Optional.empty();
        } else if (scriptOutput instanceof Boolean) {
            return Optional.of((Boolean) scriptOutput);
        }
        throw new ScriptExecutionException("Script returned unexpected value: " + scriptOutput);
    }
}
