/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.common.internal.dto.v4;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Elements that should be brought into an execution environment for a given resource (job, cluster, etc).
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
public class ExecutionEnvironment {
    private final ImmutableSet<
        @NotEmpty(message = "The config file name can't be empty")
        @Size(max = 1024, message = "Config file name is longer than 1024 characters") String> configs;
    private final ImmutableSet<
        @NotEmpty(message = "The dependency file name can't be empty")
        @Size(max = 1024, message = "Dependency file is longer than 1024 characters") String> dependencies;
    @Size(max = 1024, message = "Max length of the setup file name is 1024 characters")
    private final String setupFile;

    /**
     * Constructor.
     *
     * @param configs      Any configuration files needed for a resource at execution time. 1024 characters max for
     *                     each. Optional. Any blanks will be removed
     * @param dependencies Any dependency files needed for a resource at execution time. 1024 characters max for each.
     *                     Optional. Any blanks will be removed
     * @param setupFile    Any file that should be run to setup a resource at execution time. 1024 characters max.
     *                     Optional
     */
    @JsonCreator
    public ExecutionEnvironment(
        @JsonProperty("configs") @Nullable final Set<String> configs,
        @JsonProperty("dependencies") @Nullable final Set<String> dependencies,
        @JsonProperty("setupFile") @Nullable final String setupFile
    ) {
        this.configs = configs == null ? ImmutableSet.of() : ImmutableSet.copyOf(
            configs
                .stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet())
        );
        this.dependencies = dependencies == null ? ImmutableSet.of() : ImmutableSet.copyOf(
            dependencies
                .stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet())
        );
        this.setupFile = StringUtils.isBlank(setupFile) ? null : setupFile;
    }

    /**
     * Get the configuration files needed at runtime. The returned set will be immutable and any attempt to modify will
     * throw an exception.
     *
     * @return The set of setup file locations.
     */
    public Set<String> getConfigs() {
        return this.configs;
    }

    /**
     * Get the dependency files needed at runtime. The returned set will be immutable and any attempt to modify will
     * throw an exception.
     *
     * @return The set of dependency file locations.
     */
    public Set<String> getDependencies() {
        return this.dependencies;
    }

    /**
     * Get the setup file location if there is one.
     *
     * @return Setup file location wrapped in an {@link Optional}
     */
    public Optional<String> getSetupFile() {
        return Optional.ofNullable(this.setupFile);
    }
}
