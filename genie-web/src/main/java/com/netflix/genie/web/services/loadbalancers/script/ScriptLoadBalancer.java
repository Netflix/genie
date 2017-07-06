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
package com.netflix.genie.web.services.loadbalancers.script;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of the ClusterLoadBalancer interface which uses user a supplied script to make decisions based
 * on the list of clusters and the job request supplied.
 *
 * The contract between the script and the Java code is that the script will be supplied global variables
 * {@code clusters} and {@code jobRequest} which will be JSON strings representing the list (array) of clusters
 * matching the cluster criteria tags and the job request that kicked off this evaluation. The code expects the script
 * to either return the id of the cluster if one is selected or null if none was selected.
 *
 * @author tgianos
 * @since 3.1.0
 */
@Component
@ConditionalOnProperty(value = "genie.jobs.clusters.loadBalancers.script.enabled", havingValue = "true")
@Slf4j
public class ScriptLoadBalancer implements ClusterLoadBalancer {

    static final String SCRIPT_TIMEOUT_PROPERTY_KEY = "genie.jobs.clusters.loadBalancers.script.timeout";
    static final String SCRIPT_FILE_SOURCE_PROPERTY_KEY
        = "genie.jobs.clusters.loadBalancers.script.source";
    static final String SCRIPT_FILE_DESTINATION_PROPERTY_KEY
        = "genie.jobs.clusters.loadBalancers.script.destination";
    static final String SCRIPT_REFRESH_RATE_PROPERTY_KEY
        = "genie.jobs.clusters.loadBalancers.script.refreshRate";
    static final String SELECT_TIMER_NAME = "genie.jobs.clusters.loadBalancers.script.select.timer";
    static final String UPDATE_TIMER_NAME = "genie.jobs.clusters.loadBalancers.script.update.timer";
    static final String STATUS_TAG_KEY = "status";
    static final String STATUS_TAG_OK = "ok";
    static final String STATUS_TAG_NOT_FOUND = "not found";
    static final String STATUS_TAG_NOT_CONFIGURED = "not configured";
    static final String STATUS_TAG_FOUND = "found";
    static final String STATUS_TAG_FAILED = "failed";
    static final String EXCEPTION_TAG_KEY = "exception";

    private static final long DEFAULT_TIMEOUT_LENGTH = 5_000L;
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final String SLASH = "/";
    private static final String PERIOD = ".";
    private static final int ORDER = Ordered.LOWEST_PRECEDENCE - 1;
    private static final String CLUSTERS_BINDING = "clusters";
    private static final String JOB_REQUEST_BINDING = "jobRequest";

    private final AtomicBoolean isUpdating = new AtomicBoolean();
    private final AtomicBoolean isConfigured = new AtomicBoolean();
    private final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();

    private final AsyncTaskExecutor taskExecutor;
    private final GenieFileTransferService fileTransferService;
    private final Environment environment;
    private final ObjectMapper mapper;
    private final Registry registry;

    private CompiledScript script;
    private long timeoutLength = DEFAULT_TIMEOUT_LENGTH;

    /**
     * Constructor.
     *
     * @param taskExecutor        The asynchronous task executor to use to run the load balancer script in
     * @param taskScheduler       The task scheduler to schedule the script refresh task with
     * @param fileTransferService The file transfer service to use to download the script
     * @param environment         The program environment to get properties from
     * @param mapper              The object mapper to use to serialize objects to JSON for binding with scripts
     * @param registry            The metrics registry to use for collecting metrics
     */
    @Autowired
    public ScriptLoadBalancer(
        final AsyncTaskExecutor taskExecutor,
        final TaskScheduler taskScheduler,
        @Qualifier("cacheGenieFileTransferService") final GenieFileTransferService fileTransferService,
        final Environment environment,
        final ObjectMapper mapper,
        final Registry registry
    ) {
        this.taskExecutor = taskExecutor;
        this.fileTransferService = fileTransferService;
        this.environment = environment;
        this.mapper = mapper;
        this.registry = registry;

        // Schedule the task to run with the configured refresh rate
        // Task will be stopped when the system stops
        final long refreshRate = this.environment.getProperty(
            SCRIPT_REFRESH_RATE_PROPERTY_KEY,
            Long.class,
            300_000L
        );
        taskScheduler.scheduleWithFixedDelay(this::refresh, refreshRate);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster selectCluster(final List<Cluster> clusters, final JobRequest jobRequest) throws GenieException {
        final long selectStart = System.nanoTime();
        final Map<String, String> tags = Maps.newHashMap();
        try {
            if (this.isConfigured.get() && this.script != null) {
                final Bindings bindings = new SimpleBindings();
                bindings.put(CLUSTERS_BINDING, this.mapper.writeValueAsString(clusters));
                bindings.put(JOB_REQUEST_BINDING, this.mapper.writeValueAsString(jobRequest));

                // Run as callable and timeout after the configured timeout length
                final String clusterId = this.taskExecutor
                    .submit(() -> (String) this.script.eval(bindings))
                    .get(this.timeoutLength, TimeUnit.MILLISECONDS);

                // Find the cluster if not null
                if (clusterId != null) {
                    for (final Cluster cluster : clusters) {
                        if (cluster.getId().isPresent() && clusterId.equals(cluster.getId().get())) {
                            tags.put(STATUS_TAG_KEY, STATUS_TAG_FOUND);
                            return cluster;
                        }
                    }
                }
            } else {
                tags.put(STATUS_TAG_KEY, STATUS_TAG_NOT_CONFIGURED);
                return null;
            }

            tags.put(STATUS_TAG_KEY, STATUS_TAG_NOT_FOUND);
            // Defer to any subsequent load balancer in the chain
            return null;
        } catch (final Exception e) {
            tags.put(STATUS_TAG_KEY, STATUS_TAG_FAILED);
            tags.put(EXCEPTION_TAG_KEY, e.getClass().getName());
            log.error("Unable to execute script due to", e.getMessage(), e);
            return null;
        } finally {
            this.registry
                .timer(this.registry.createId(SELECT_TIMER_NAME, tags))
                .record(System.nanoTime() - selectStart, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getOrder() {
        return ORDER;
    }

    /**
     * Check if the script file needs to be refreshed.
     */
    public void refresh() {
        final long updateStart = System.nanoTime();
        this.isUpdating.set(true);

        // Update the script timeout
        this.timeoutLength = this.environment.getProperty(
            SCRIPT_TIMEOUT_PROPERTY_KEY,
            Long.class,
            DEFAULT_TIMEOUT_LENGTH
        );

        final Map<String, String> tags = Maps.newHashMap();
        try {
            final String scriptFileSource = this.environment.getProperty(SCRIPT_FILE_SOURCE_PROPERTY_KEY);
            if (scriptFileSource == null) {
                throw new IllegalStateException(
                    "No script source file found for property " + SCRIPT_FILE_SOURCE_PROPERTY_KEY
                );
            }
            final String scriptDestinationDir = this.environment.getProperty(SCRIPT_FILE_DESTINATION_PROPERTY_KEY);
            if (scriptDestinationDir == null) {
                throw new IllegalStateException(
                    "No script destination directory found for property " + SCRIPT_FILE_DESTINATION_PROPERTY_KEY
                );
            }
            final String fileName = StringUtils.substringAfterLast(scriptFileSource, SLASH);
            if (StringUtils.isBlank(fileName)) {
                throw new IllegalStateException("No file name found from " + scriptFileSource);
            }

            final String scriptExtension = StringUtils.substringAfterLast(fileName, PERIOD);
            if (StringUtils.isBlank(scriptExtension)) {
                throw new IllegalStateException("No file extension available in " + fileName);
            }

            final String scriptFileDestination = scriptDestinationDir.endsWith(SLASH)
                ? scriptDestinationDir + fileName
                : scriptDestinationDir + SLASH + fileName;

            // Download and cache the file (if it's not already there)
            this.fileTransferService.getFile(scriptFileSource, scriptFileDestination);

            final ScriptEngine engine = this.scriptEngineManager.getEngineByExtension(scriptExtension);
            // We want a compilable engine so we can cache the script
            if (!(engine instanceof Compilable)) {
                throw new IllegalArgumentException(
                    "Script engine must be of type " + Compilable.class.getName()
                );
            }
            final Compilable compilable = (Compilable) engine;
            try (
                final FileInputStream fis = new FileInputStream(scriptFileDestination);
                final InputStreamReader reader = new InputStreamReader(fis, UTF_8)
            ) {
                this.script = compilable.compile(reader);
            }

            tags.put(STATUS_TAG_KEY, STATUS_TAG_OK);

            this.isConfigured.set(true);
        } catch (final GenieException | IOException | ScriptException | RuntimeException e) {
            tags.put(STATUS_TAG_KEY, STATUS_TAG_FAILED);
            tags.put(EXCEPTION_TAG_KEY, e.getClass().getName());
            log.error(
                "Refreshing the load balancing script for ScriptLoadBalancer failed due to {}",
                e.getMessage(),
                e
            );
            this.isConfigured.set(false);
        } finally {
            this.isUpdating.set(false);
            this.registry
                .timer(this.registry.createId(UPDATE_TIMER_NAME, tags))
                .record(System.nanoTime() - updateStart, TimeUnit.NANOSECONDS);
        }
    }
}
