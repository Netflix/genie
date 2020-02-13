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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.CommandMetadata;
import com.netflix.genie.common.external.dtos.v4.CommandStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.properties.CommandSelectorManagedScriptProperties;
import com.netflix.genie.web.properties.ScriptManagerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

import javax.script.ScriptEngineManager;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class CommandSelectorManagedScriptIntegrationTest {

    private static final ExecutionResourceCriteria CRITERIA = new ExecutionResourceCriteria(
        Lists.newArrayList(new Criterion.Builder().withId(UUID.randomUUID().toString()).build()),
        new Criterion.Builder().withName(UUID.randomUUID().toString()).build(),
        null
    );
    private static final JobMetadata JOB_METADATA = new JobMetadata.Builder(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()
    ).build();

    private static final Command COMMAND_0 = createTestCommand("0");
    private static final Command COMMAND_1 = createTestCommand("1");
    private static final Command COMMAND_2 = createTestCommand("2");

    private static final JobRequest JOB_REQUEST_0 = createTestJobRequest("0");
    private static final JobRequest JOB_REQUEST_1 = createTestJobRequest("1");
    private static final JobRequest JOB_REQUEST_2 = createTestJobRequest("2");
    private static final JobRequest JOB_REQUEST_3 = createTestJobRequest("3");
    private static final JobRequest JOB_REQUEST_4 = createTestJobRequest("4");

    private static final Set<Command> COMMANDS = Sets.newHashSet(COMMAND_0, COMMAND_1, COMMAND_2);

    private CommandSelectorManagedScriptProperties scriptProperties;
    private CommandSelectorManagedScript commandSelectorManagedScript;
    private MeterRegistry meterRegistry;
    private ExecutorService executorService;

    private static Command createTestCommand(final String id) {
        return new Command(
            id,
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new CommandMetadata.Builder(
                "d",
                "e",
                "f",
                CommandStatus.ACTIVE
            ).build(),
            Lists.newArrayList(UUID.randomUUID().toString()),
            null,
            0L,
            null
        );
    }

    private static JobRequest createTestJobRequest(final String requestedId) {
        return new JobRequest(
            requestedId,
            null,
            null,
            JOB_METADATA,
            CRITERIA,
            null,
            null,
            null
        );
    }

    @BeforeEach
    void setUp() {
        this.meterRegistry = new SimpleMeterRegistry();
        this.executorService = Executors.newCachedThreadPool();
        final ScriptManager scriptManager = new ScriptManager(
            new ScriptManagerProperties(),
            new ConcurrentTaskScheduler(),
            this.executorService,
            new ScriptEngineManager(),
            new DefaultResourceLoader(),
            this.meterRegistry
        );
        this.scriptProperties = new CommandSelectorManagedScriptProperties();
        this.commandSelectorManagedScript = new CommandSelectorManagedScript(
            scriptManager,
            this.scriptProperties,
            GenieObjectMapper.getMapper(),
            this.meterRegistry
        );
    }

    @AfterEach
    void tearDown() {
        this.meterRegistry.close();
        this.executorService.shutdownNow();
    }

    @Test
    void selectCommandTest() throws Exception {
        ManagedScriptIntegrationTest.loadScript(
            "selectCommand.groovy",
            this.commandSelectorManagedScript,
            this.scriptProperties
        );

        CommandSelectorManagedScript.CommandSelectionResult result;

        result = this.commandSelectorManagedScript.selectCommand(COMMANDS, JOB_REQUEST_0);
        Assertions.assertThat(result.getCommand()).isPresent().contains(COMMAND_0);
        Assertions.assertThat(result.getRationale()).isPresent().contains("selected 0");

        result = this.commandSelectorManagedScript.selectCommand(COMMANDS, JOB_REQUEST_1);
        Assertions.assertThat(result.getCommand()).isNotPresent();
        Assertions.assertThat(result.getRationale()).isPresent().contains("Couldn't find anything");

        Assertions
            .assertThatExceptionOfType(ResourceSelectionException.class)
            .isThrownBy(() -> this.commandSelectorManagedScript.selectCommand(COMMANDS, JOB_REQUEST_2))
            .withNoCause();

        result = this.commandSelectorManagedScript.selectCommand(COMMANDS, JOB_REQUEST_3);
        Assertions.assertThat(result.getCommand()).isNotPresent();
        Assertions.assertThat(result.getRationale()).isNotPresent();

        Assertions
            .assertThatExceptionOfType(ResourceSelectionException.class)
            .isThrownBy(() -> this.commandSelectorManagedScript.selectCommand(COMMANDS, JOB_REQUEST_4))
            .withCauseInstanceOf(ScriptExecutionException.class);
    }
}
