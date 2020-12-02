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
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.CommandMetadata;
import com.netflix.genie.common.external.dtos.v4.CommandStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.internal.util.PropertiesMapCache;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.properties.CommandSelectorManagedScriptProperties;
import com.netflix.genie.web.properties.ScriptManagerProperties;
import com.netflix.genie.web.selectors.CommandSelectionContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

import javax.script.ScriptEngineManager;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

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

    private static final String JOB_REQUEST_0_ID = "0";
    private static final String JOB_REQUEST_1_ID = "1";
    private static final String JOB_REQUEST_2_ID = "2";
    private static final String JOB_REQUEST_3_ID = "3";
    private static final String JOB_REQUEST_4_ID = "4";
    private static final String JOB_REQUEST_5_ID = "5";

    private static final JobRequest JOB_REQUEST_0 = createTestJobRequest(JOB_REQUEST_0_ID);
    private static final JobRequest JOB_REQUEST_1 = createTestJobRequest(JOB_REQUEST_1_ID);
    private static final JobRequest JOB_REQUEST_2 = createTestJobRequest(JOB_REQUEST_2_ID);
    private static final JobRequest JOB_REQUEST_3 = createTestJobRequest(JOB_REQUEST_3_ID);
    private static final JobRequest JOB_REQUEST_4 = createTestJobRequest(JOB_REQUEST_4_ID);
    private static final JobRequest JOB_REQUEST_5 = createTestJobRequest(JOB_REQUEST_5_ID);

    private static final Set<Command> COMMANDS = Sets.newHashSet(COMMAND_0, COMMAND_1, COMMAND_2);
    private static final Set<Cluster> CLUSTERS = Sets.newHashSet(
        Mockito.mock(Cluster.class),
        Mockito.mock(Cluster.class)
    );
    private static final Map<Command, Set<Cluster>> COMMAND_CLUSTERS = COMMANDS
        .stream()
        .collect(Collectors.toMap(command -> command, command -> CLUSTERS));

    private CommandSelectorManagedScriptProperties scriptProperties;
    private CommandSelectorManagedScript commandSelectorManagedScript;
    private MeterRegistry meterRegistry;
    private ExecutorService executorService;
    private PropertiesMapCache cache;

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
        this.cache = Mockito.mock(PropertiesMapCache.class);
        this.commandSelectorManagedScript = new CommandSelectorManagedScript(
            scriptManager,
            this.scriptProperties,
            this.meterRegistry,
            this.cache
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

        ResourceSelectorScriptResult<Command> result;

        result = this.commandSelectorManagedScript.selectResource(this.createContext(JOB_REQUEST_0_ID));
        Assertions.assertThat(result.getResource()).isPresent().contains(COMMAND_0);
        Assertions.assertThat(result.getRationale()).isPresent().contains("selected 0");

        result = this.commandSelectorManagedScript.selectResource(this.createContext(JOB_REQUEST_1_ID));
        Assertions.assertThat(result.getResource()).isNotPresent();
        Assertions.assertThat(result.getRationale()).isPresent().contains("Couldn't find anything");

        Assertions
            .assertThatExceptionOfType(ResourceSelectionException.class)
            .isThrownBy(() -> this.commandSelectorManagedScript.selectResource(this.createContext(JOB_REQUEST_2_ID)))
            .withNoCause();

        result = this.commandSelectorManagedScript.selectResource(this.createContext(JOB_REQUEST_3_ID));
        Assertions.assertThat(result.getResource()).isNotPresent();
        Assertions.assertThat(result.getRationale()).isNotPresent();

        Assertions
            .assertThatExceptionOfType(ResourceSelectionException.class)
            .isThrownBy(() -> this.commandSelectorManagedScript.selectResource(this.createContext(JOB_REQUEST_4_ID)))
            .withCauseInstanceOf(ScriptExecutionException.class);

        // Invalid return type from script
        Assertions
            .assertThatExceptionOfType(ResourceSelectionException.class)
            .isThrownBy(() -> this.commandSelectorManagedScript.selectResource(this.createContext(JOB_REQUEST_5_ID)));
    }

    private CommandSelectionContext createContext(final String jobId) {
        final JobRequest jobRequest;
        final boolean apiJob;
        switch (jobId) {
            case JOB_REQUEST_0_ID:
                apiJob = true;
                jobRequest = JOB_REQUEST_0;
                break;
            case JOB_REQUEST_1_ID:
                apiJob = false;
                jobRequest = JOB_REQUEST_1;
                break;
            case JOB_REQUEST_2_ID:
                apiJob = false;
                jobRequest = JOB_REQUEST_2;
                break;
            case JOB_REQUEST_3_ID:
                apiJob = true;
                jobRequest = JOB_REQUEST_3;
                break;
            case JOB_REQUEST_4_ID:
                apiJob = false;
                jobRequest = JOB_REQUEST_4;
                break;
            case JOB_REQUEST_5_ID:
                apiJob = true;
                jobRequest = JOB_REQUEST_5;
                break;
            default:
                throw new IllegalArgumentException(jobId + " is currently unsupported");
        }
        return new CommandSelectionContext(
            jobId,
            jobRequest,
            apiJob,
            COMMAND_CLUSTERS
        );
    }
}
