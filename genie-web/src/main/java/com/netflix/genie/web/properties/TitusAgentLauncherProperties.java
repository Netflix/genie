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
     * Name of the property that enables {@link TitusAgentLauncherImpl}.
     */
    public static final String ENABLE_PROPERTY = PREFIX + ".enabled";

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
    @NotEmpty(message = "The command-line launch template cannot be empty")
    private List<@NotBlank String> entryPointTemplate = Arrays.asList(
        "/bin/genie-agent",
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
}
