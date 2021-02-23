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
package com.netflix.genie.web.agent.launchers.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.dtos.TitusBatchJobRequest;
import com.netflix.genie.web.dtos.TitusBatchJobResponse;
import com.netflix.genie.web.exceptions.checked.AgentLaunchException;
import com.netflix.genie.web.properties.TitusAgentLauncherProperties;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.util.unit.DataSize;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Agent launcher that spawns a job in a dedicated container through Titus (https://netflix.github.io/titus/).
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class TitusAgentLauncherImpl implements AgentLauncher {

    static final int MEGABYTE_TO_MEGABIT = 8;
    private static final String GENIE_USER_ATTR = "genie.user";
    private static final String GENIE_SOURCE_HOST_ATTR = "genie.sourceHost";
    private static final String GENIE_ENDPOINT_ATTR = "genie.endpoint";
    private static final String GENIE_JOB_ID_ATTR = "genie.jobId";
    private static final String TITUS_API_JOB_PATH = "/api/v3/jobs";
    private static final String TITUS_JOB_ID_EXT_FIELD = "titusId";
    private static final String TITUS_JOB_REQUEST_EXT_FIELD = "titusRequest";
    private static final String TITUS_JOB_RESPONSE_EXT_FIELD = "titusResponse";
    private static final String THIS_CLASS = TitusAgentLauncherImpl.class.getCanonicalName();
    private static final Tag CLASS_TAG = Tag.of(LAUNCHER_CLASS_KEY, THIS_CLASS);
    private static final int TITUS_JOB_BATCH_SIZE = 1;
    private static final int ZERO = 0;
    private final RestTemplate restTemplate;
    private final Cache<String, String> healthIndicatorCache;
    private final GenieHostInfo genieHostInfo;
    private final TitusAgentLauncherProperties titusAgentLauncherProperties;
    private final Environment environment;
    private final boolean hasDataSizeConverters;
    private final Binder binder;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param restTemplate                 the rest template
     * @param healthIndicatorCache         a cache to store metadata about recently launched jobs
     * @param genieHostInfo                the metadata about the local server and host
     * @param titusAgentLauncherProperties the configuration properties
     * @param environment                  The application environment to pull dynamic properties from
     * @param registry                     the metric registry
     */
    public TitusAgentLauncherImpl(
        final RestTemplate restTemplate,
        final Cache<String, String> healthIndicatorCache,
        final GenieHostInfo genieHostInfo,
        final TitusAgentLauncherProperties titusAgentLauncherProperties,
        final Environment environment,
        final MeterRegistry registry
    ) {
        this.restTemplate = restTemplate;
        this.healthIndicatorCache = healthIndicatorCache;
        this.genieHostInfo = genieHostInfo;
        this.titusAgentLauncherProperties = titusAgentLauncherProperties;
        this.environment = environment;
        if (this.environment instanceof ConfigurableEnvironment) {
            final ConfigurableEnvironment configurableEnvironment = (ConfigurableEnvironment) this.environment;
            final ConversionService conversionService = configurableEnvironment.getConversionService();
            this.hasDataSizeConverters
                = conversionService.canConvert(String.class, DataSize.class)
                && conversionService.canConvert(Integer.class, DataSize.class);
        } else {
            this.hasDataSizeConverters = false;
        }
        this.binder = Binder.get(this.environment);
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<JsonNode> launchAgent(
        final ResolvedJob resolvedJob,
        @Nullable final JsonNode requestedLauncherExt
    ) throws AgentLaunchException {
        final long start = System.nanoTime();
        log.info("Received request to launch Titus agent to run job: {}", resolvedJob);
        final Set<Tag> tags = new HashSet<>();
        tags.add(CLASS_TAG);

        final String jobId = resolvedJob.getJobSpecification().getJob().getId();

        final TitusBatchJobRequest titusJobRequest = this.createJobRequest(resolvedJob);

        String titusJobId = null;

        try {
            final TitusBatchJobResponse titusResponse = this.restTemplate.postForObject(
                this.titusAgentLauncherProperties.getEndpoint().toString() + TITUS_API_JOB_PATH,
                titusJobRequest,
                TitusBatchJobResponse.class
            );

            if (titusResponse == null) {
                throw new AgentLaunchException("Failed to request creation of Titus job for job " + jobId);
            }

            titusJobId = titusResponse.getId();
            if (StringUtils.isBlank(titusJobId)) {
                throw new AgentLaunchException(
                    "Failed to create titus job for job " + jobId
                        + " - " + titusResponse.getStatusCode() + ": "
                        + titusResponse.getMessage()
                );
            }

            log.info("Created Titus job {} to execute Genie job {}", titusJobId, jobId);

            MetricsUtils.addSuccessTags(tags);

            return Optional.of(
                JsonNodeFactory.instance.objectNode()
                    .put(LAUNCHER_CLASS_EXT_FIELD, THIS_CLASS)
                    .put(SOURCE_HOST_EXT_FIELD, this.genieHostInfo.getHostname())
                    .put(TITUS_JOB_ID_EXT_FIELD, titusJobId)
                    .putPOJO(TITUS_JOB_REQUEST_EXT_FIELD, titusJobRequest)
                    .putPOJO(TITUS_JOB_RESPONSE_EXT_FIELD, titusResponse)
            );
        } catch (Exception e) {
            log.error("Failed to launch job on Titus", e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new AgentLaunchException("Failed to create titus job for job " + jobId, e);
        } finally {
            this.registry.timer(LAUNCH_TIMER, tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            this.healthIndicatorCache.put(jobId, StringUtils.isBlank(titusJobId) ? "-" : titusJobId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Health health() {
        return Health.up()
            .withDetails(this.healthIndicatorCache.asMap())
            .build();
    }

    private TitusBatchJobRequest createJobRequest(final ResolvedJob resolvedJob) {
        final String jobId = resolvedJob.getJobSpecification().getJob().getId();

        // Map placeholders in entry point template to their values
        final Map<String, String> placeholdersMap = ImmutableMap.of(
            TitusAgentLauncherProperties.JOB_ID_PLACEHOLDER,
            jobId,
            TitusAgentLauncherProperties.SERVER_HOST_PLACEHOLDER,
            this.titusAgentLauncherProperties.getGenieServerHost(),
            TitusAgentLauncherProperties.SERVER_PORT_PLACEHOLDER,
            String.valueOf(this.titusAgentLauncherProperties.getGenieServerPort())
        );

        // Substitute all placeholders with their values
        final List<String> entryPoint =
            this.titusAgentLauncherProperties.getEntryPointTemplate()
                .stream()
                .map(s -> placeholdersMap.getOrDefault(s, s))
                .collect(Collectors.toList());

        final long memory = Math.max(
            this.getDataSizeProperty(
                TitusAgentLauncherProperties.MINIMUM_MEMORY_PROPERTY,
                this.titusAgentLauncherProperties.getMinimumMemory()
            ).toMegabytes(),
            resolvedJob.getJobEnvironment().getMemory() + this.getDataSizeProperty(
                TitusAgentLauncherProperties.ADDITIONAL_MEMORY_PROPERTY,
                this.titusAgentLauncherProperties.getAdditionalMemory()
            ).toMegabytes()
        );
        final int cpu = Math.max(
            this.environment.getProperty(
                TitusAgentLauncherProperties.MINIMUM_CPU_PROPERTY,
                Integer.class,
                this.titusAgentLauncherProperties.getMinimumCPU()
            ),
            resolvedJob.getJobEnvironment().getCpu() + this.environment.getProperty(
                TitusAgentLauncherProperties.ADDITIONAL_CPU_PROPERTY,
                Integer.class,
                this.titusAgentLauncherProperties.getAdditionalCPU()
            )
        );
        final long diskSize = Math.max(
            this.getDataSizeProperty(
                TitusAgentLauncherProperties.MINIMUM_DISK_SIZE_PROPERTY,
                this.titusAgentLauncherProperties.getMinimumDiskSize()
            ).toMegabytes(),
            ZERO /* TODO: Placeholder for job request field */ + this.getDataSizeProperty(
                TitusAgentLauncherProperties.ADDITIONAL_DISK_SIZE_PROPERTY,
                this.titusAgentLauncherProperties.getAdditionalDiskSize()
            ).toMegabytes()
        );
        final long networkMbps = Math.max(
            this.getDataSizeProperty(
                TitusAgentLauncherProperties.MINIMUM_BANDWIDTH_PROPERTY,
                this.titusAgentLauncherProperties.getMinimumBandwidth()
            ).toMegabytes(),
            ZERO /* TODO: Placeholder for job request field */ + this.getDataSizeProperty(
                TitusAgentLauncherProperties.ADDITIONAL_BANDWIDTH_PROPERTY,
                this.titusAgentLauncherProperties.getAdditionalBandwidth()
            ).toMegabytes()
        ) * MEGABYTE_TO_MEGABIT;
        final int gpus = Math.max(
            this.environment.getProperty(
                TitusAgentLauncherProperties.MINIMUM_GPU_PROPERTY,
                Integer.class,
                this.titusAgentLauncherProperties.getMinimumGPU()
            ),
            ZERO /* TODO: Placeholder for job request field */ + this.environment.getProperty(
                TitusAgentLauncherProperties.ADDITIONAL_GPU_PROPERTY,
                Integer.class,
                this.titusAgentLauncherProperties.getAdditionalGPU()
            )
        );
        final Duration runtimeLimit = this.titusAgentLauncherProperties.getRuntimeLimit();

        return new TitusBatchJobRequest(
            new TitusBatchJobRequest.Owner(this.titusAgentLauncherProperties.getOwnerEmail()),
            this.titusAgentLauncherProperties.getApplicationName(),
            this.environment.getProperty(
                TitusAgentLauncherProperties.CAPACITY_GROUP_PROPERTY,
                String.class,
                this.titusAgentLauncherProperties.getCapacityGroup()
            ),
            ImmutableMap.of(
                GENIE_USER_ATTR, resolvedJob.getJobMetadata().getUser(),
                GENIE_SOURCE_HOST_ATTR, this.genieHostInfo.getHostname(),
                GENIE_ENDPOINT_ATTR, this.titusAgentLauncherProperties.getGenieServerHost(),
                GENIE_JOB_ID_ATTR, jobId
            ),
            new TitusBatchJobRequest.Container(
                new TitusBatchJobRequest.Resources(
                    cpu,
                    gpus,
                    memory,
                    diskSize,
                    networkMbps
                ),
                new TitusBatchJobRequest.SecurityProfile(
                    this.titusAgentLauncherProperties.getSecurityAttributes(),
                    this.titusAgentLauncherProperties.getSecurityGroups(),
                    this.titusAgentLauncherProperties.getIAmRole()
                ),
                new TitusBatchJobRequest.Image(
                    this.environment.getProperty(
                        TitusAgentLauncherProperties.IMAGE_NAME_PROPERTY,
                        String.class,
                        this.titusAgentLauncherProperties.getImageName()
                    ),
                    this.environment.getProperty(
                        TitusAgentLauncherProperties.IMAGE_TAG_PROPERTY,
                        String.class,
                        this.titusAgentLauncherProperties.getImageTag()
                    )
                ),
                entryPoint,
                this.binder
                    .bind(
                        TitusAgentLauncherProperties.ADDITIONAL_ENVIRONMENT_PROPERTY,
                        Bindable.mapOf(String.class, String.class)
                    )
                    .orElse(new HashMap<>())
            ),
            new TitusBatchJobRequest.Batch(
                TITUS_JOB_BATCH_SIZE,
                new TitusBatchJobRequest.RetryPolicy(
                    new TitusBatchJobRequest.Immediate(
                        this.environment.getProperty(
                            TitusAgentLauncherProperties.RETRIES_PROPERTY,
                            Integer.class,
                            this.titusAgentLauncherProperties.getRetries()
                        )
                    )
                ),
                runtimeLimit.getSeconds()
            ),
            new TitusBatchJobRequest.DisruptionBudget(new TitusBatchJobRequest.SelfManaged(runtimeLimit.toMillis()))
        );
    }

    /**
     * Helper method to avoid runtime errors if for some reason the DataSize converters aren't loaded.
     *
     * @param propertyKey  The key to get from the environment
     * @param defaultValue The default value to apply
     * @return The resolved value
     */
    private DataSize getDataSizeProperty(final String propertyKey, final DataSize defaultValue) {
        if (this.hasDataSizeConverters) {
            return this.environment.getProperty(propertyKey, DataSize.class, defaultValue);
        } else {
            final String propValue = this.environment.getProperty(propertyKey);
            if (propValue != null) {
                try {
                    return DataSize.parse(propValue);
                } catch (final IllegalArgumentException e) {
                    log.error(
                        "Unable to parse value of {} as DataSize. Falling back to default value {}",
                        propertyKey,
                        defaultValue,
                        e
                    );
                }
            }
            return defaultValue;
        }
    }
}
