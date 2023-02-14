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
package com.netflix.genie.web.properties;

import com.netflix.genie.web.agent.launchers.impl.TitusAgentLauncherImpl;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration properties for the {@link TitusAgentLauncherImpl}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = TitusAgentLauncherProperties.PREFIX)
@Getter
@Setter
@Validated
public class TitusAgentLauncherProperties {

    /**
     * Properties prefix.
     */
    public static final String PREFIX = "genie.agent.launcher.titus";

    /**
     * An additional amount of overhead bandwidth that should be added to whatever the job originally requested.
     */
    public static final String ADDITIONAL_BANDWIDTH_PROPERTY = PREFIX + ".additionalBandwidth";

    /**
     * An additional number of CPUs that should be added to whatever the job originally requested.
     */
    public static final String ADDITIONAL_CPU_PROPERTY = PREFIX + ".additionalCPU";

    /**
     * An additional amount of disk space that should be added to whatever the job originally requested.
     */
    public static final String ADDITIONAL_DISK_SIZE_PROPERTY = PREFIX + ".additionalDiskSize";

    /**
     * Any additional environment variables that should be inserted into the container runtime.
     */
    public static final String ADDITIONAL_ENVIRONMENT_PROPERTY = PREFIX + ".additional-environment";

    /**
     * An additional number of GPUs that should be added to whatever the job originally requested.
     */
    public static final String ADDITIONAL_GPU_PROPERTY = PREFIX + ".additionalGPU";

    /**
     * Any additional job attributes that should be added to defaults.
     */
    public static final String ADDITIONAL_JOB_ATTRIBUTES_PROPERTY = PREFIX + ".additional-job-attributes";

    /**
     * An additional amount of memory that should be added to whatever the job originally requested.
     */
    public static final String ADDITIONAL_MEMORY_PROPERTY = PREFIX + ".additionalMemory";

    /**
     * The capacity group to launch Genie containers in Titus with.
     */
    public static final String CAPACITY_GROUP_PROPERTY = PREFIX + ".capacityGroup";

    /**
     * Any attributes that should be added to the request specifically for the container.
     */
    public static final String CONTAINER_ATTRIBUTES_PROPERTY = PREFIX + ".container-attributes";

    /**
     * Name of the property that enables {@link TitusAgentLauncherImpl}.
     */
    public static final String ENABLE_PROPERTY = PREFIX + ".enabled";

    /**
     * The name of the property that dictates which image to launch on Titus with.
     */
    public static final String IMAGE_NAME_PROPERTY = PREFIX + ".imageName";

    /**
     * The name of the property that dictates which image tag to launch on Titus with.
     */
    public static final String IMAGE_TAG_PROPERTY = PREFIX + ".imageTag";

    /**
     * The minimum network bandwidth to allocate to the container.
     */
    public static final String MINIMUM_BANDWIDTH_PROPERTY = PREFIX + ".minimumBandwidth";

    /**
     * The minimum number of CPUs any container should launch with.
     */
    public static final String MINIMUM_CPU_PROPERTY = PREFIX + ".minimumCPU";

    /**
     * The minimum size of the disk volume to attach to the job container.
     */
    public static final String MINIMUM_DISK_SIZE_PROPERTY = PREFIX + ".minimumDiskSize";

    /**
     * The minimum number of GPUs any container should launch with.
     */
    public static final String MINIMUM_GPU_PROPERTY = PREFIX + ".minimumGPU";

    /**
     * The minimum amount of memory a container should be allocated.
     */
    public static final String MINIMUM_MEMORY_PROPERTY = PREFIX + ".minimumMemory";

    /**
     * The number of retries to make on the Titus submission.
     */
    public static final String RETRIES_PROPERTY = PREFIX + ".retries";

    /**
     * The max duration a Titus job is allowed to run after Genie has submitted it.
     */
    public static final String RUNTIME_LIMIT = PREFIX + ".runtimeLimit";

    /**
     * Placeholder for job id for use in entry point expression.
     */
    public static final String JOB_ID_PLACEHOLDER = "<JOB_ID>";

    /**
     * Placeholder for Genie server hostname id for use in entry point expression.
     */
    public static final String SERVER_HOST_PLACEHOLDER = "<SERVER_HOST>";

    /**
     * Placeholder for Genie server port id for use in entry point expression.
     */
    public static final String SERVER_PORT_PLACEHOLDER = "<SERVER_PORT>";

    /**
     * The property containing the Agent image key to look up the corresponding image metadata within.
     */
    public static final String AGENT_IMAGE_KEY_PROPERTY = PREFIX + ".agentImageKey";

    /**
     * The property for titus container network mode.
     */
    public static final String CONTAINER_NETWORK_CONFIGURATION = PREFIX + ".networkConfiguration";

    /**
     * Whether the Titus Agent Launcher is enabled.
     */
    private boolean enabled;

    /**
     * Titus REST endpoint.
     */
    private URI endpoint = URI.create("https://example-titus-endpoint.tld:1234");

    /**
     * The Titus job container entry point.
     * Placeholder values are substituted before submission
     */
    private List<@NotBlank String> entryPointTemplate = Arrays.asList("/bin/genie-agent");

    /**
     * The Titus job container command array.
     * Placeholder values are substituted before submission
     */
    private List<@NotBlank String> commandTemplate = Arrays.asList(
        "exec",
        "--api-job",
        "--launchInJobDirectory",
        "--job-id",
        JOB_ID_PLACEHOLDER,
        "--server-host",
        SERVER_HOST_PLACEHOLDER,
        "--server-port",
        SERVER_PORT_PLACEHOLDER
    );

    /**
     * The Titus job owner.
     */
    @NotEmpty
    private String ownerEmail = "owners@genie.tld";

    /**
     * The Titus application name.
     */
    @NotEmpty
    private String applicationName = "genie";

    /**
     * The Titus capacity group.
     */
    @NotEmpty
    private String capacityGroup = "default";

    /**
     * A map of security attributes.
     */
    @NotNull
    private Map<String, String> securityAttributes = new HashMap<>();

    /**
     * A list of security groups.
     */
    @NotNull
    private List<String> securityGroups = new ArrayList<>();

    /**
     * The IAM role.
     */
    @NotEmpty
    private String iAmRole = "arn:aws:iam::000000000:role/SomeProfile";

    /**
     * The image name.
     */
    @NotEmpty
    private String imageName = "image-name";

    /**
     * The image tag.
     */
    @NotEmpty
    private String imageTag = "latest";

    /**
     * The number of retries if the job fails.
     */
    @Min(0)
    private int retries;

    /**
     * The job runtime limit (also applied to the used for disruption budget).
     */
    @DurationMin
    private Duration runtimeLimit = Duration.ofHours(12);

    /**
     * The Genie server hostname for the agent to connect to.
     */
    @NotEmpty
    private String genieServerHost = "example.genie.tld";

    /**
     * The Genie server port for the agent to connect to.
     */
    @Min(0)
    private int genieServerPort = 9090;

    /**
     * The maximum size of the list of jobs displayed by the health indicator.
     */
    @Min(0)
    private int healthIndicatorMaxSize = 100;

    /**
     * The maximum time a job is retained in the health indicator list.
     */
    @DurationMin
    private Duration healthIndicatorExpiration = Duration.ofMinutes(30);

    /**
     * Additional environment variables to set.
     */
    @NotNull
    private Map<String, String> additionalEnvironment = new HashMap<>();

    /**
     * The amount of bandwidth to request in addition to the amount requested by the job.
     */
    private DataSize additionalBandwidth = DataSize.ofBytes(0);

    /**
     * The amount of CPUs to request in addition to the amount requested by the job.
     */
    @Min(0)
    private int additionalCPU = 1;

    /**
     * The amount of disk space to request in addition to the amount requested by the job.
     */
    private DataSize additionalDiskSize = DataSize.ofGigabytes(1);

    /**
     * The amount of GPUs to request in addition to the amount requested by the job.
     */
    @Min(0)
    private int additionalGPU;

    /**
     * The amount of memory to request in addition to the amount requested by the job.
     */
    private DataSize additionalMemory = DataSize.ofGigabytes(2);

    /**
     * The minimum amount of bandwidth to request for the container.
     */
    @NotNull
    private DataSize minimumBandwidth = DataSize.ofMegabytes(7);

    /**
     * The minimum amount of CPUs to request for the container.
     */
    @Min(1)
    private int minimumCPU = 1;

    /**
     * The minimum amount of storage to request for the container.
     */
    @NotNull
    private DataSize minimumDiskSize = DataSize.ofGigabytes(10);

    /**
     * The minimum amount of GPUs to request for the container.
     */
    @Min(0)
    private int minimumGPU;

    /**
     * The minimum amount of memory to request for the container.
     */
    @NotNull
    private DataSize minimumMemory = DataSize.ofGigabytes(4);

    /**
     * A map of container attributes.
     */
    @NotNull
    private Map<String, String> containerAttributes = new HashMap<>();

    /**
     * Additional job attributes.
     */
    @NotNull
    private Map<String, String> additionalJobAttributes = new HashMap<>();

    /**
     * The stack (jobGroupInfo) within the application space for Titus request.
     */
    private String stack = "";

    /**
     * The detail (jobGroupInfo) within the application space for Titus request.
     */
    private String detail = "";

    /**
     * The sequence (jobGroupInfo) within the application space for Titus request.
     */
    private String sequence = "";

    /**
     * The key within the images block that corresponds to the image housing the Genie agent binary.
     */
    private String agentImageKey = "genieAgent";
}
