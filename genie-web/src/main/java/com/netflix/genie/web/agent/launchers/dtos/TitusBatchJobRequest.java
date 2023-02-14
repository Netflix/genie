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
package com.netflix.genie.web.agent.launchers.dtos;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.apache.logging.log4j.util.Strings;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * Titus job request POJO.
 *
 * @author mprimi
 * @since 4.0.0
 */
// TODO: This class and the nested classes likely could benefit from a builder pattern but I'm just too busy to tackle
//       this right now so going to do some hacky things to get migration unblocked. Also it's very hardcoded to be
//       specifically what is needed for Titus API calls within Netflix and not perhaps Titus api calls in general.
//       An example of this is iAmRole is required but if the OSS Titus isn't run on AWS or that field isn't actually
//       required by the API what should someone put here? The API needs to be reviewed and fields that are optional
//       made optional and those that are required made clear they are required. Right now everything is required or
//       unclear. Making everything mutable for now.
//       - TJG 2/26/2020
@Data
@Builder
public class TitusBatchJobRequest {

    @NotNull
    @NonNull
    private Owner owner;
    @NotNull
    @NonNull
    private Map<String, String> attributes;
    @NotNull
    @NonNull
    private Container container;
    @NotNull
    @NonNull
    private Batch batch;
    @NotNull
    @NonNull
    private DisruptionBudget disruptionBudget;
    @NotNull
    @NonNull
    private String applicationName;
    @NotNull
    @NonNull
    private String capacityGroup;
    @NotNull
    @NonNull
    private JobGroupInfo jobGroupInfo;
    @Nullable
    private NetworkConfiguration networkConfiguration;

    /**
     * Titus job owner POJO.
     */
    @Data
    @Builder
    public static class Owner {
        @NotNull
        @NonNull
        private String teamEmail;
    }

    /**
     * Titus job container DTO.
     */
    @Data
    @Builder
    public static class Container {
        @NotNull
        @NotNull
        private Resources resources;
        @NotNull
        @NonNull
        private SecurityProfile securityProfile;
        @NotNull
        @NonNull
        private Image image;
        @NotNull
        @NonNull
        private List<String> entryPoint;
        @NotNull
        @NonNull
        private List<String> command;
        @NotNull
        @NonNull
        private Map<String, String> env;
        @NotNull
        @NonNull
        private Map<String, String> attributes;
        @Nullable
        private ContainerConstraints softConstraints;
        @Nullable
        private ContainerConstraints hardConstraints;
    }

    /**
     * Titus job container resources POJO.
     */
    @Data
    @Builder
    public static class Resources {
        private int cpu;
        private int gpu;
        private long memoryMB;
        private long diskMB;
        private long networkMbps;
    }

    /**
     * Titus job security profile.
     */
    @Data
    @Builder
    public static class SecurityProfile {
        @NotNull
        @NonNull
        private Map<String, String> attributes;
        @NotNull
        @NonNull
        private List<String> securityGroups;
        @NotNull
        @NonNull
        private String iamRole;
    }

    /**
     * Titus job container constraints.
     */
    @Data
    @Builder
    public static class ContainerConstraints {
        @NotNull
        @NonNull
        private Map<String, String> constraints;

        @NotNull
        @NonNull
        @Builder.Default()
        private String expression = Strings.EMPTY;
    }

    /**
     * Titus job container image.
     */
    @Data
    @Builder
    public static class Image {
        @NotEmpty
        @NonNull
        private String name;
        @NotEmpty
        @NonNull
        private String tag;
    }

    /**
     * Titus batch job parameters.
     */
    @Data
    @Builder
    public static class Batch {
        @NotNull
        @NonNull
        private RetryPolicy retryPolicy;
        @Min(1)
        private int size;
        private long runtimeLimitSec;
    }

    /**
     * Titus job network configuration.
     */
    @Data
    @Builder
    public static class NetworkConfiguration {
        private String networkMode;
    }

    /**
     * Titus job disruption budget.
     */
    @Data
    @Builder
    public static class DisruptionBudget {
        @NotNull
        @NonNull
        private SelfManaged selfManaged;
    }

    /**
     * Titus job retry policy.
     */
    @Data
    @Builder
    public static class RetryPolicy {
        @NotNull
        @NonNull
        private Immediate immediate;
    }

    /**
     * Titus job retry policy detail.
     */
    @Data
    @Builder
    public static class Immediate {
        @Min(0)
        private int retries;
    }

    /**
     * Titus job disruption budget detail.
     */
    @Data
    @Builder
    public static class SelfManaged {
        @Min(1)
        private long relocationTimeMs;
    }

    /**
     * Job Group information.
     */
    @Data
    @Builder
    public static class JobGroupInfo {
        private String stack;
        private String detail;
        private String sequence;
    }
}

