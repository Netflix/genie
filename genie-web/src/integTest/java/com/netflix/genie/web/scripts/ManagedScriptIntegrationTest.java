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
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import com.netflix.genie.web.properties.ScriptManagerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

import javax.script.ScriptEngineManager;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

class ManagedScriptIntegrationTest {

    private TestScriptProperties scriptProperties;
    private TestScript script;

    static void loadScript(
        final String scriptFilename,
        final ManagedScript script,
        final ManagedScriptBaseProperties scriptProperties
    ) throws Exception {
        // Find the script resource
        final URI scriptUri = ExecutionModeFilterScript.class.getResource(scriptFilename).toURI();
        // Configure script to use it
        scriptProperties.setSource(scriptUri);
        // Trigger loading of script
        scriptProperties.setAutoLoadEnabled(true);
        script.warmUp();

        // Wait for script to be ready to evaluate
        final Instant deadline = Instant.now().plus(10, ChronoUnit.SECONDS);
        while (!script.isReadyToEvaluate()) {
            if (Instant.now().isAfter(deadline)) {
                throw new RuntimeException("Timed out waiting for script to load");
            }
            System.out.println("Script not loaded yet...");
            Thread.sleep(500);
        }
        System.out.println("Script loaded");
    }

    @BeforeEach
    void setUp() {
        final MeterRegistry meterRegistry = new SimpleMeterRegistry();
        final ScriptManagerProperties scriptManagerProperties = new ScriptManagerProperties();
        final TaskScheduler taskScheduler = new ConcurrentTaskScheduler();
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
        this.scriptProperties = new TestScriptProperties();
        this.script = new TestScript(
            scriptManager,
            scriptProperties,
            objectMapper,
            meterRegistry
        );
    }

    @ParameterizedTest(name = "Evaluate NOOP script: {0}")
    @ValueSource(strings = {"noop.js", "noop.groovy"})
    void evaluateSuccessfully(
        final String scriptFilename
    ) throws Exception {
        ManagedScriptIntegrationTest.loadScript(scriptFilename, script, scriptProperties);
        this.script.evaluate();
    }

    @ParameterizedTest(name = "Timeout evaluating {0}")
    @ValueSource(strings = {"sleep.js", "sleep.groovy"})
    void scriptEvaluationTimeoutTest(
        final String scriptFilename
    ) throws Exception {
        ManagedScriptIntegrationTest.loadScript(scriptFilename, script, scriptProperties);

        this.scriptProperties.setTimeout(1);

        Assertions
            .assertThatThrownBy(() -> script.evaluate())
            .isInstanceOf(ScriptExecutionException.class)
            .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void evaluateScriptNotLoadedTest() {
        Assertions
            .assertThatThrownBy(() -> script.evaluate())
            .isInstanceOf(ScriptNotConfiguredException.class);
    }

    @Test
    void validateArgsGroovyScriptTest() throws Exception {
        ManagedScriptIntegrationTest.loadScript("validate-args.groovy", script, scriptProperties);

        final ImmutableMap<String, Object> arguments = ImmutableMap.of(
            "booleanArg", Boolean.TRUE,
            "stringArg", "Foo",
            "integerArg", Integer.valueOf(678)
        );

        Assertions
            .assertThat(script.evaluateScript(arguments))
            .isEqualTo(arguments);
    }

    private static class TestScript extends ManagedScript {
        TestScript(
            final ScriptManager scriptManager,
            final ManagedScriptBaseProperties properties,
            final ObjectMapper mapper, final MeterRegistry registry
        ) {
            super(scriptManager, properties, mapper, registry);
        }

        void evaluate() throws ScriptNotConfiguredException, ScriptExecutionException {
            super.evaluateScript(ImmutableMap.of());
        }
    }

    private static class TestScriptProperties extends ManagedScriptBaseProperties {
    }
}
