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

import brave.Span;
import brave.Tracer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.base.Strings;
import com.netflix.genie.common.internal.dtos.ComputeResources;
import com.netflix.genie.common.internal.dtos.Image;
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.agent.launchers.dtos.TitusBatchJobRequest;
import com.netflix.genie.web.agent.launchers.dtos.TitusBatchJobResponse;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.AgentLaunchException;
import com.netflix.genie.web.properties.TitusAgentLauncherProperties;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.classify.Classifier;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.ExceptionClassifierRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.unit.DataSize;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Agent launcher that spawns a job in a dedicated container through Titus.
 *
 * @author mprimi
 * @see <a href="https://netflix.github.io/titus/">Titus OSS Project</a>
 * @since 4.0.0
 */
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
    private static final int DEFAULT_JOB_CPU = 1;
    private static final int DEFAULT_JOB_GPU = 0;
    private static final long DEFAULT_JOB_MEMORY = 1_536L;
    private static final long DEFAULT_JOB_DISK = 10_000L;
    private static final long DEFAULT_JOB_NETWORK = 16_000L;
    private static final BiFunction<List<String>, Map<String, String>, List<String>> REPLACE_PLACEHOLDERS =
        (template, placeholders) -> template
            .stream()
            .map(s -> placeholders.getOrDefault(s, s))
            .collect(Collectors.toList());
    private static final Logger LOG = LoggerFactory.getLogger(TitusAgentLauncherImpl.class);

    private final RestTemplate restTemplate;
    private final RetryTemplate retryTemplate;
    private final Cache<String, String> healthIndicatorCache;
    private final GenieHostInfo genieHostInfo;
    private final TitusAgentLauncherProperties titusAgentLauncherProperties;
    private final Environment environment;
    private final TitusJobRequestAdapter jobRequestAdapter;
    private final boolean hasDataSizeConverters;
    private final Binder binder;
    private final MeterRegistry registry;
    private final Tracer tracer;
    private final BraveTracePropagator tracePropagator;

    /**
     * Constructor.
     *
     * @param restTemplate                 the rest template
     * @param retryTemplate                The {@link RetryTemplate} to use when making Titus API calls
     * @param jobRequestAdapter            The implementation of {@link TitusJobRequestAdapter} to use
     * @param healthIndicatorCache         a cache to store metadata about recently launched jobs
     * @param genieHostInfo                the metadata about the local server and host
     * @param titusAgentLauncherProperties the configuration properties
     * @param tracingComponents            The {@link BraveTracingComponents} instance to use for distributed tracing
     * @param environment                  The application environment to pull dynamic properties from
     * @param registry                     the metric registry
     */
    public TitusAgentLauncherImpl(
        final RestTemplate restTemplate,
        final RetryTemplate retryTemplate,
        final TitusJobRequestAdapter jobRequestAdapter,
        final Cache<String, String> healthIndicatorCache,
        final GenieHostInfo genieHostInfo,
        final TitusAgentLauncherProperties titusAgentLauncherProperties,
        final BraveTracingComponents tracingComponents,
        final Environment environment,
        final MeterRegistry registry
    ) {
        this.restTemplate = restTemplate;
        this.retryTemplate = retryTemplate;
        this.healthIndicatorCache = healthIndicatorCache;
        this.genieHostInfo = genieHostInfo;
        this.titusAgentLauncherProperties = titusAgentLauncherProperties;
        this.jobRequestAdapter = jobRequestAdapter;
        this.tracer = tracingComponents.getTracer();
        this.tracePropagator = tracingComponents.getTracePropagator();
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
        LOG.info("Received request to launch Titus agent to run job: {}", resolvedJob);
        final Set<Tag> tags = new HashSet<>();
        tags.add(CLASS_TAG);

        final String jobId = resolvedJob.getJobSpecification().getJob().getId();
        String titusJobId = null;

        try {
            final TitusBatchJobRequest titusJobRequest = this.createJobRequest(resolvedJob);
            final TitusBatchJobResponse titusResponse = this.retryTemplate.execute(
                (RetryCallback<TitusBatchJobResponse, Throwable>) context -> restTemplate.postForObject(
                    titusAgentLauncherProperties.getEndpoint().toString() + TITUS_API_JOB_PATH,
                    titusJobRequest,
                    TitusBatchJobResponse.class
                )
            );

            if (titusResponse == null) {
                throw new AgentLaunchException("Failed to request creation of Titus job for job " + jobId);
            }

            titusJobId = titusResponse.getId().orElseThrow(
                () -> new AgentLaunchException(
                    "Failed to create titus job for job "
                        + jobId
                        + " - Titus Status Code:"
                        + titusResponse.getStatusCode().orElse(null)
                        + ", Titus response message:"
                        + titusResponse.getMessage().orElse("")
                )
            );

            LOG.info("Created Titus job {} to execute Genie job {}", titusJobId, jobId);

            MetricsUtils.addSuccessTags(tags);

            return Optional.of(
                JsonNodeFactory.instance.objectNode()
                    .put(LAUNCHER_CLASS_EXT_FIELD, THIS_CLASS)
                    .put(SOURCE_HOST_EXT_FIELD, this.genieHostInfo.getHostname())
                    .put(TITUS_JOB_ID_EXT_FIELD, titusJobId)
                    .putPOJO(TITUS_JOB_REQUEST_EXT_FIELD, titusJobRequest)
                    .putPOJO(TITUS_JOB_RESPONSE_EXT_FIELD, titusResponse)
            );
        } catch (Throwable t) {
            LOG.error("Failed to launch job on Titus", t);
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new AgentLaunchException("Failed to create titus job for job " + jobId, t);
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

    private TitusBatchJobRequest createJobRequest(final ResolvedJob resolvedJob) throws AgentLaunchException {
        final String jobId = resolvedJob.getJobSpecification().getJob().getId();

        // Map placeholders in entry point template to their values
        final Map<String, String> placeholdersMap = Map.of(
            TitusAgentLauncherProperties.JOB_ID_PLACEHOLDER,
            jobId,
            TitusAgentLauncherProperties.SERVER_HOST_PLACEHOLDER,
            this.titusAgentLauncherProperties.getGenieServerHost(),
            TitusAgentLauncherProperties.SERVER_PORT_PLACEHOLDER,
            String.valueOf(this.titusAgentLauncherProperties.getGenieServerPort())
        );

        // Substitute all placeholders with their values for the container entry point and command
        final List<String> entryPoint = REPLACE_PLACEHOLDERS.apply(
            this.titusAgentLauncherProperties.getEntryPointTemplate(),
            placeholdersMap
        );
        final List<String> command = REPLACE_PLACEHOLDERS.apply(
            this.titusAgentLauncherProperties.getCommandTemplate(),
            placeholdersMap
        );
        final Duration runtimeLimit = this.titusAgentLauncherProperties.getRuntimeLimit();

        final Map<String, String> jobAttributes = this.createJobAttributes(jobId, resolvedJob);

        final TitusBatchJobRequest.TitusBatchJobRequestBuilder requestBuilder = TitusBatchJobRequest.builder()
            .owner(
                TitusBatchJobRequest.Owner
                    .builder()
                    .teamEmail(this.titusAgentLauncherProperties.getOwnerEmail())
                    .build()
            )
            .applicationName(this.titusAgentLauncherProperties.getApplicationName())
            .capacityGroup(
                this.environment.getProperty(
                    TitusAgentLauncherProperties.CAPACITY_GROUP_PROPERTY,
                    String.class,
                    this.titusAgentLauncherProperties.getCapacityGroup()
                )
            )
            .attributes(jobAttributes)
            .container(
                TitusBatchJobRequest.Container
                    .builder()
                    .resources(this.getTitusResources(resolvedJob))
                    .securityProfile(
                        TitusBatchJobRequest.SecurityProfile.builder()
                            .attributes(new HashMap<>(this.titusAgentLauncherProperties.getSecurityAttributes()))
                            .securityGroups(new ArrayList<>(this.titusAgentLauncherProperties.getSecurityGroups()))
                            .iamRole(this.titusAgentLauncherProperties.getIAmRole())
                            .build()
                    )
                    .image(this.getTitusImage(resolvedJob))
                    .entryPoint(entryPoint)
                    .command(command)
                    .env(this.createJobEnvironment())
                    .attributes(
                        this.binder
                            .bind(
                                TitusAgentLauncherProperties.CONTAINER_ATTRIBUTES_PROPERTY,
                                Bindable.mapOf(String.class, String.class)
                            )
                            .orElse(new HashMap<>())
                    )
                    .build()
            )
            .batch(
                TitusBatchJobRequest.Batch.builder()
                    .size(TITUS_JOB_BATCH_SIZE)
                    .retryPolicy(
                        TitusBatchJobRequest.RetryPolicy.builder()
                            .immediate(
                                TitusBatchJobRequest.Immediate
                                    .builder()
                                    .retries(
                                        this.environment.getProperty(
                                            TitusAgentLauncherProperties.RETRIES_PROPERTY,
                                            Integer.class,
                                            this.titusAgentLauncherProperties.getRetries()
                                        )
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .runtimeLimitSec(runtimeLimit.getSeconds())
                    .build()
            )
            .disruptionBudget(
                TitusBatchJobRequest.DisruptionBudget.builder()
                    .selfManaged(
                        TitusBatchJobRequest.SelfManaged.builder()
                            .relocationTimeMs(runtimeLimit.toMillis())
                            .build()
                    )
                    .build()
            )
            .jobGroupInfo(
                TitusBatchJobRequest.JobGroupInfo.builder()
                    .stack(this.titusAgentLauncherProperties.getStack())
                    .detail(this.titusAgentLauncherProperties.getDetail())
                    .sequence(this.titusAgentLauncherProperties.getSequence())
                    .build()
            );

        final Optional<String> networkConfiguration = validateNetworkConfiguration(this.environment.getProperty(
            TitusAgentLauncherProperties.CONTAINER_NETWORK_CONFIGURATION,
            String.class));
        networkConfiguration.ifPresent(config -> requestBuilder.networkConfiguration(
                TitusBatchJobRequest.NetworkConfiguration.builder().networkMode(config).build()));

        final TitusBatchJobRequest request = requestBuilder.build();

        // Run the request through the security adapter to add any necessary context
        this.jobRequestAdapter.modifyJobRequest(request, resolvedJob);
        return request;
    }

    private Optional<String> validateNetworkConfiguration(@Nullable final String networkConfig) {
        if (Strings.isNullOrEmpty(networkConfig)) {
            return Optional.empty();
        }

        switch (networkConfig) {
            case "Ipv4Only":
            case "Ipv6AndIpv4":
            case "Ipv6AndIpv4Fallback":
            case "Ipv6Only":
            case "HighScale":
                return Optional.of(networkConfig);
            default:
                return Optional.empty();
        }

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
                    LOG.error(
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

    private Map<String, String> createJobAttributes(final String jobId, final ResolvedJob resolvedJob) {
        final Map<String, String> jobAttributes = new HashMap<>();
        jobAttributes.put(GENIE_USER_ATTR, resolvedJob.getJobMetadata().getUser());
        jobAttributes.put(GENIE_SOURCE_HOST_ATTR, this.genieHostInfo.getHostname());
        jobAttributes.put(GENIE_ENDPOINT_ATTR, this.titusAgentLauncherProperties.getGenieServerHost());
        jobAttributes.put(GENIE_JOB_ID_ATTR, jobId);
        jobAttributes.putAll(
            this.binder
                .bind(
                    TitusAgentLauncherProperties.ADDITIONAL_JOB_ATTRIBUTES_PROPERTY,
                    Bindable.mapOf(String.class, String.class)
                )
                .orElse(new HashMap<>())
        );
        return jobAttributes;
    }

    private Map<String, String> createJobEnvironment() {
        final Map<String, String> jobEnvironment = this.binder
            .bind(
                TitusAgentLauncherProperties.ADDITIONAL_ENVIRONMENT_PROPERTY,
                Bindable.mapOf(String.class, String.class)
            )
            .orElse(new HashMap<>());

        final Span currentSpan = this.tracer.currentSpan();
        if (currentSpan != null) {
            jobEnvironment.putAll(this.tracePropagator.injectForAgent(currentSpan.context()));
        }

        return jobEnvironment;
    }

    private TitusBatchJobRequest.Resources getTitusResources(final ResolvedJob resolvedJob) {
        // TODO: Address defaults?
        final ComputeResources computeResources = resolvedJob.getJobEnvironment().getComputeResources();
        final int cpu = Math.max(
            this.environment.getProperty(
                TitusAgentLauncherProperties.MINIMUM_CPU_PROPERTY,
                Integer.class,
                this.titusAgentLauncherProperties.getMinimumCPU()
            ),
            computeResources.getCpu().orElse(DEFAULT_JOB_CPU) + this.environment.getProperty(
                TitusAgentLauncherProperties.ADDITIONAL_CPU_PROPERTY,
                Integer.class,
                this.titusAgentLauncherProperties.getAdditionalCPU()
            )
        );
        final int gpus = Math.max(
            this.environment.getProperty(
                TitusAgentLauncherProperties.MINIMUM_GPU_PROPERTY,
                Integer.class,
                this.titusAgentLauncherProperties.getMinimumGPU()
            ),
            computeResources.getGpu().orElse(DEFAULT_JOB_GPU) + this.environment.getProperty(
                TitusAgentLauncherProperties.ADDITIONAL_GPU_PROPERTY,
                Integer.class,
                this.titusAgentLauncherProperties.getAdditionalGPU()
            )
        );
        final long memory = Math.max(
            this.getDataSizeProperty(
                TitusAgentLauncherProperties.MINIMUM_MEMORY_PROPERTY,
                this.titusAgentLauncherProperties.getMinimumMemory()
            ).toMegabytes(),
            computeResources.getMemoryMb().orElse(DEFAULT_JOB_MEMORY) + this.getDataSizeProperty(
                TitusAgentLauncherProperties.ADDITIONAL_MEMORY_PROPERTY,
                this.titusAgentLauncherProperties.getAdditionalMemory()
            ).toMegabytes()
        );
        final long diskSize = Math.max(
            this.getDataSizeProperty(
                TitusAgentLauncherProperties.MINIMUM_DISK_SIZE_PROPERTY,
                this.titusAgentLauncherProperties.getMinimumDiskSize()
            ).toMegabytes(),
            computeResources.getDiskMb().orElse(DEFAULT_JOB_DISK) + this.getDataSizeProperty(
                TitusAgentLauncherProperties.ADDITIONAL_DISK_SIZE_PROPERTY,
                this.titusAgentLauncherProperties.getAdditionalDiskSize()
            ).toMegabytes()
        );
        final long networkMbps = Math.max(
            this.getDataSizeProperty(
                TitusAgentLauncherProperties.MINIMUM_BANDWIDTH_PROPERTY,
                this.titusAgentLauncherProperties.getMinimumBandwidth()
            ).toMegabytes() * MEGABYTE_TO_MEGABIT,
            computeResources.getNetworkMbps().orElse(DEFAULT_JOB_NETWORK) + this.getDataSizeProperty(
                TitusAgentLauncherProperties.ADDITIONAL_BANDWIDTH_PROPERTY,
                this.titusAgentLauncherProperties.getAdditionalBandwidth()
            ).toMegabytes() * MEGABYTE_TO_MEGABIT
        );

        return TitusBatchJobRequest.Resources.builder()
            .cpu(cpu)
            .gpu(gpus)
            .memoryMB(memory)
            .diskMB(diskSize)
            .networkMbps(networkMbps)
            .build();
    }

    private TitusBatchJobRequest.Image getTitusImage(final ResolvedJob resolvedJob) {
        final Map<String, Image> images = resolvedJob.getJobEnvironment().getImages();

        final String defaultImageName = this.environment.getProperty(
            TitusAgentLauncherProperties.IMAGE_NAME_PROPERTY,
            String.class,
            this.titusAgentLauncherProperties.getImageName()
        );
        final String defaultImageTag = this.environment.getProperty(
            TitusAgentLauncherProperties.IMAGE_TAG_PROPERTY,
            String.class,
            this.titusAgentLauncherProperties.getImageTag()
        );
        final Image image = images.getOrDefault(
            this.environment.getProperty(
                TitusAgentLauncherProperties.AGENT_IMAGE_KEY_PROPERTY,
                String.class,
                this.titusAgentLauncherProperties.getAgentImageKey()
            ),
            new Image.Builder()
                .withName(defaultImageName)
                .withTag(defaultImageTag)
                .build()
        );

        return TitusBatchJobRequest.Image.builder()
            .name(image.getName().orElse(defaultImageName))
            .tag(image.getTag().orElse(defaultImageTag))
            .build();
    }

    /**
     * An interface that should be implemented by any class which wants to modify the Titus job request before it is
     * sent.
     * <p>
     * NOTE: This is a very initial implementation/idea and highly subject to change as we work through additional
     * security concerns
     *
     * @author tgianos
     * @since 4.0.0
     */
    public interface TitusJobRequestAdapter {

        /**
         * Given the current {@link TitusBatchJobRequest} and the {@link ResolvedJob} that the agent container
         * is expected to execute this method should manipulate (if necessary) the {@literal request} as needed for the
         * given Titus installation Genie is calling.
         *
         * @param request     The {@link TitusBatchJobRequest} state after everything default has been set and created
         *                    and is ready to be sent to the Titus jobs API
         * @param resolvedJob The Genie {@link ResolvedJob} that the Titus request is responsible for executing
         * @throws AgentLaunchException For any errors
         */
        default void modifyJobRequest(
            TitusBatchJobRequest request,
            ResolvedJob resolvedJob
        ) throws AgentLaunchException {
        }
    }

    /**
     * A retry policy that has different behavior based on the type of exception thrown by the rest client during
     * calls to the Titus API.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class TitusAPIRetryPolicy extends ExceptionClassifierRetryPolicy {

        private static final long serialVersionUID = -7978685711081275362L;

        /**
         * Constructor.
         *
         * @param retryCodes  The {@link HttpStatus} codes which should be retried if an API call to Titus fails
         * @param maxAttempts The maximum number of retry attempts that should be made upon call failure
         */
        public TitusAPIRetryPolicy(final Set<HttpStatus> retryCodes, final int maxAttempts) {
            final NeverRetryPolicy neverRetryPolicy = new NeverRetryPolicy();
            final SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(maxAttempts);

            this.setExceptionClassifier(
                (Classifier<Throwable, RetryPolicy>) classifiable -> {
                    if (classifiable instanceof HttpStatusCodeException) {
                        final HttpStatusCodeException httpException = (HttpStatusCodeException) classifiable;
                        final HttpStatus status = httpException.getStatusCode();
                        if (retryCodes.contains(status)) {
                            return simpleRetryPolicy;
                        }
                    }

                    return neverRetryPolicy;
                }
            );
        }
    }
}
