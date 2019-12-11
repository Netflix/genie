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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.properties.ExecutionModeFilterScriptProperties;
import com.netflix.genie.web.properties.ScriptManagerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

import javax.script.ScriptEngineManager;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

class ExecutionModeFilterScriptIntegrationTest {

    private static final String TEST_SCRIPT_NAME = "execution-mode-filter.groovy";

    private ExecutionModeFilterScriptProperties scriptProperties;
    private ExecutionModeFilterScript executionModeFilterScript;

    @BeforeEach
    void setUp() {
        final MeterRegistry meterRegistry = new SimpleMeterRegistry();
        final ScriptManagerProperties scriptManagerProperties = new ScriptManagerProperties();
        final TaskScheduler taskScheduler =  new ConcurrentTaskScheduler();
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        final ResourceLoader resourceLoader = new DefaultResourceLoader();
        final ObjectMapper objectMapper = GenieObjectMapper.getMapper();
        final ScriptManager scriptManager = new ScriptManager(
            scriptManagerProperties,
            taskScheduler,
            executorService,
            scriptEngineManager,
            resourceLoader,
            meterRegistry
        );
        this.scriptProperties = new ExecutionModeFilterScriptProperties();
        this.executionModeFilterScript = new ExecutionModeFilterScript(
            scriptManager,
            scriptProperties,
            objectMapper,
            meterRegistry
        );
    }

    private static Stream<Arguments> getEvaluateTestArguments() {
        return Stream.of(
            Arguments.of(Optional.of(true)),
            Arguments.of(Optional.of(false)),
            Arguments.of(Optional.empty())
        );
    }

    @ParameterizedTest(name = "Script returns: {0}")
    @MethodSource("getEvaluateTestArguments")
    void evaluateTest(
        final Optional<Boolean> expected
    ) throws Exception {
        ManagedScriptIntegrationTest.loadScript(TEST_SCRIPT_NAME, executionModeFilterScript, scriptProperties);

        final JobRequest jobRequest = new JobRequest.Builder(
            "jobName",
            "jobUser",
            "jobVersion",
            Lists.newArrayList(
                new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            ),
            Sets.newHashSet(UUID.randomUUID().toString())
        )
            .withDescription("Script should return: " + expected.orElse(null))
            .build();

        Assertions.assertThat(
            this.executionModeFilterScript.forceAgentExecution(jobRequest)
        ).isEqualTo(expected);
    }

    @Test
    void evaluateErrorTest() throws Exception {
        final JobRequest jobRequest = new JobRequest.Builder(
            "jobName",
            "jobUser",
            "jobVersion",
            Lists.newArrayList(
                new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            ),
            Sets.newHashSet(UUID.randomUUID().toString())
        )
            .build();

        ManagedScriptIntegrationTest.loadScript(TEST_SCRIPT_NAME, executionModeFilterScript, scriptProperties);
        Assertions
            .assertThatExceptionOfType(ScriptExecutionException.class)
            .isThrownBy(() -> this.executionModeFilterScript.forceAgentExecution(jobRequest));
    }
}
