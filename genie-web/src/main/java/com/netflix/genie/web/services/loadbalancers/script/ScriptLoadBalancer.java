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
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.web.controllers.DtoConverters;
import com.netflix.genie.web.properties.ScriptLoadBalancerProperties;
import com.netflix.genie.web.services.ClusterLoadBalancer;
import com.netflix.genie.web.services.impl.GenieFileTransferService;
import com.netflix.genie.web.util.MetricsConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.Environment;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;

import javax.annotation.Nonnull;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import javax.validation.constraints.NotEmpty;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * An implementation of the ClusterLoadBalancer interface which uses user a supplied script to make decisions based
 * on the list of clusters and the job request supplied.
 * <p>
 * The contract between the script and the Java code is that the script will be supplied global variables
 * {@code clusters} and {@code jobRequest} which will be JSON strings representing the list (array) of clusters
 * matching the cluster criteria tags and the job request that kicked off this evaluation. The code expects the script
 * to either return the id of the cluster if one is selected or null if none was selected.
 *
 * @author tgianos
 * @since 3.1.0
 */
@Slf4j
public class ScriptLoadBalancer implements ClusterLoadBalancer {

    static final String SELECT_TIMER_NAME = "genie.jobs.clusters.loadBalancers.script.select.timer";
    static final String UPDATE_TIMER_NAME = "genie.jobs.clusters.loadBalancers.script.update.timer";
    static final String STATUS_TAG_OK = "ok";
    static final String STATUS_TAG_NOT_FOUND = "not found";
    static final String STATUS_TAG_NOT_CONFIGURED = "not configured";
    static final String STATUS_TAG_FOUND = "found";
    static final String STATUS_TAG_FAILED = "failed";
    private static final long DEFAULT_TIMEOUT_LENGTH = 5_000L;
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final String SLASH = "/";
    private static final String PERIOD = ".";
    private static final String CLUSTERS_BINDING = "clusters";
    private static final String JOB_REQUEST_BINDING = "jobRequest";

    private final AtomicBoolean isUpdating = new AtomicBoolean();
    private final AtomicBoolean isConfigured = new AtomicBoolean();
    private final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();

    private final AsyncTaskExecutor asyncTaskExecutor;
    private final GenieFileTransferService fileTransferService;
    private final Environment environment;
    private final ObjectMapper mapper;
    private final MeterRegistry registry;

    private final AtomicReference<CompiledScript> script = new AtomicReference<>(null);
    private final AtomicLong timeoutLength = new AtomicLong(DEFAULT_TIMEOUT_LENGTH);

    /**
     * Constructor.
     *
     * @param asyncTaskExecutor   The asynchronous task executor to use to run the load balancer script in
     * @param taskScheduler       The task scheduler to schedule the script refresh task with
     * @param fileTransferService The file transfer service to use to download the script
     * @param environment         The program environment to get properties from
     * @param mapper              The object mapper to use to serialize objects to JSON for binding with scripts
     * @param registry            The metrics registry to use for collecting metrics
     */
    public ScriptLoadBalancer(
        final AsyncTaskExecutor asyncTaskExecutor,
        final TaskScheduler taskScheduler,
        final GenieFileTransferService fileTransferService,
        final Environment environment,
        final ObjectMapper mapper,
        final MeterRegistry registry
    ) {
        this.asyncTaskExecutor = asyncTaskExecutor;
        this.fileTransferService = fileTransferService;
        this.environment = environment;
        this.mapper = mapper;
        this.registry = registry;

        // Schedule the task to run with the configured refresh rate
        // Task will be stopped when the system stops
        final long refreshRate = this.environment.getProperty(
            ScriptLoadBalancerProperties.REFRESH_RATE_PROPERTY,
            Long.class,
            300_000L
        );
        taskScheduler.scheduleWithFixedDelay(this::refresh, refreshRate);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster selectCluster(
        @Nonnull @NonNull @NotEmpty final Set<Cluster> clusters,
        @Nonnull @NonNull final JobRequest jobRequest
    ) throws GenieException {
        final long selectStart = System.nanoTime();
        log.debug("Called");
        final Set<Tag> tags = Sets.newHashSet();
        try {
            if (this.isConfigured.get() && this.script.get() != null) {
                log.debug("Evaluating script for job {}", jobRequest.getId().orElse("without id"));
                final Bindings bindings = new SimpleBindings();
                // TODO: For now for backwards compatibility with balancer scripts continue writing Clusters out in
                //       V3 format. Change to V4 once stabalize a bit more
                bindings.put(
                    CLUSTERS_BINDING,
                    this.mapper.writeValueAsString(
                        clusters
                            .stream()
                            .map(DtoConverters::toV3Cluster)
                            .collect(Collectors.toSet())
                    )
                );
                bindings.put(JOB_REQUEST_BINDING, this.mapper.writeValueAsString(jobRequest));

                // Run as callable and timeout after the configured timeout length
                final String clusterId = this.asyncTaskExecutor
                    .submit(() -> (String) this.script.get().eval(bindings))
                    .get(this.timeoutLength.get(), TimeUnit.MILLISECONDS);

                // Find the cluster if not null
                if (clusterId != null) {
                    for (final Cluster cluster : clusters) {
                        if (clusterId.equals(cluster.getId())) {
                            tags.add(Tag.of(MetricsConstants.TagKeys.STATUS, STATUS_TAG_FOUND));
                            return cluster;
                        }
                    }
                }
                log.warn("Script returned a cluster not in the input list: {}", clusterId);
            } else {
                log.debug("Script returned null");
                tags.add(Tag.of(MetricsConstants.TagKeys.STATUS, STATUS_TAG_NOT_CONFIGURED));
                return null;
            }

            tags.add(Tag.of(MetricsConstants.TagKeys.STATUS, STATUS_TAG_NOT_FOUND));
            // Defer to any subsequent load balancer in the chain
            return null;
        } catch (final Exception e) {
            tags.add(Tag.of(MetricsConstants.TagKeys.STATUS, STATUS_TAG_FAILED));
            tags.add(Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, e.getClass().getCanonicalName()));
            log.error("Unable to execute script due to {}", e.getMessage(), e);
            return null;
        } finally {
            this.registry
                .timer(SELECT_TIMER_NAME, tags)
                .record(System.nanoTime() - selectStart, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Check if the script file needs to be refreshed.
     */
    public void refresh() {
        log.debug("Refreshing");
        final long updateStart = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            this.isUpdating.set(true);

            // Update the script timeout
            this.timeoutLength.set(
                this.environment.getProperty(
                    ScriptLoadBalancerProperties.TIMEOUT_PROPERTY,
                    Long.class,
                    DEFAULT_TIMEOUT_LENGTH
                )
            );

            final String scriptFileSourceValue = this.environment
                .getProperty(ScriptLoadBalancerProperties.SCRIPT_FILE_SOURCE_PROPERTY);
            if (StringUtils.isBlank(scriptFileSourceValue)) {
                throw new IllegalStateException(
                    "Invalid empty value for script source file property: "
                        + ScriptLoadBalancerProperties.SCRIPT_FILE_SOURCE_PROPERTY
                );
            }
            final String scriptFileSource = new URI(scriptFileSourceValue).toString();

            final String scriptFileDestinationValue =
                this.environment.getProperty(ScriptLoadBalancerProperties.SCRIPT_FILE_DESTINATION_PROPERTY);
            if (StringUtils.isBlank(scriptFileDestinationValue)) {
                throw new IllegalStateException(
                    "Invalid empty value for script destination directory property: "
                        + ScriptLoadBalancerProperties.SCRIPT_FILE_DESTINATION_PROPERTY
                );
            }
            final Path scriptDestinationDirectory = Paths.get(new URI(scriptFileDestinationValue));

            // Check the validity of the destination directory
            if (!Files.exists(scriptDestinationDirectory)) {
                Files.createDirectories(scriptDestinationDirectory);
            } else if (!Files.isDirectory(scriptDestinationDirectory)) {
                throw new IllegalStateException(
                    "The script destination directory " + scriptDestinationDirectory + " exists but is not a directory"
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

            final Path scriptDestinationPath = scriptDestinationDirectory.resolve(fileName);

            // Download and cache the file (if it's not already there)
            this.fileTransferService.getFile(scriptFileSource, scriptDestinationPath.toUri().toString());

            final ScriptEngine engine = this.scriptEngineManager.getEngineByExtension(scriptExtension);
            // We want a compilable engine so we can cache the script
            if (!(engine instanceof Compilable)) {
                throw new IllegalArgumentException(
                    "Script engine must be of type " + Compilable.class.getName()
                );
            }
            final Compilable compilable = (Compilable) engine;
            try (
                final InputStream fis = Files.newInputStream(scriptDestinationPath);
                final InputStreamReader reader = new InputStreamReader(fis, UTF_8)
            ) {
                log.debug("Compiling {}", scriptFileSource);
                this.script.set(compilable.compile(reader));
            }

            tags.add(Tag.of(MetricsConstants.TagKeys.STATUS, STATUS_TAG_OK));

            this.isConfigured.set(true);
        } catch (final GenieException | IOException | ScriptException | RuntimeException | URISyntaxException e) {
            tags.add(Tag.of(MetricsConstants.TagKeys.STATUS, STATUS_TAG_FAILED));
            tags.add(Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, e.getClass().getName()));
            log.error(
                "Refreshing the load balancing script for ScriptLoadBalancer failed due to {}",
                e.getMessage(),
                e
            );
            this.isConfigured.set(false);
        } finally {
            this.isUpdating.set(false);
            this.registry
                .timer(UPDATE_TIMER_NAME, tags)
                .record(System.nanoTime() - updateStart, TimeUnit.NANOSECONDS);
            log.debug("Refresh completed");
        }
    }
}
