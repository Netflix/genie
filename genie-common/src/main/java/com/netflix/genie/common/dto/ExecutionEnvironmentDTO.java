/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.common.dto;

import lombok.Getter;

import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Base class DTO for DTOs which require a setup file.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
public abstract class ExecutionEnvironmentDTO extends CommonDTO {

    private static final long serialVersionUID = 2116254045303538065L;
    private final Set<String> configs = new HashSet<>();
    private final Set<String> dependencies = new HashSet<>();
    @Size(max = 1024, message = "Max length of the setup file is 1024 characters")
    private final String setupFile;

    /**
     * Constructor.
     *
     * @param builder The builder to use
     */
    @SuppressWarnings("unchecked")
    protected ExecutionEnvironmentDTO(final Builder builder) {
        super(builder);
        this.setupFile = builder.bSetupFile;
        this.configs.addAll(builder.bConfigs);
        this.dependencies.addAll(builder.bDependencies);
    }

    /**
     * Get the setup file.
     *
     * @return The setup file location as an Optional
     */
    public Optional<String> getSetupFile() {
        return Optional.ofNullable(this.setupFile);
    }

    /**
     * Get the set of configs for the entity.
     *
     * @return The configs for the entity as a read-only set.
     */
    public Set<String> getConfigs() {
        return Collections.unmodifiableSet(this.configs);
    }

    /**
     * Get the set of dependencies for the entity.
     *
     * @return The dependencies for the entity as a read-only set.
     */
    public Set<String> getDependencies() {
        return Collections.unmodifiableSet(this.dependencies);
    }

    /**
     * A builder for helping to create instances.
     *
     * @param <T> The type of builder that extends this builder for final implementation
     * @author tgianos
     * @since 3.0.0
     */
    @SuppressWarnings("unchecked")
    protected abstract static class Builder<T extends Builder> extends CommonDTO.Builder<T> {

        private final Set<String> bConfigs = new HashSet<>();
        private final Set<String> bDependencies = new HashSet<>();
        private String bSetupFile;

        /**
         * Constructor with required fields.
         *
         * @param name    The name to use for the resource
         * @param user    The user to user for the resource
         * @param version The version to use for the resource
         */
        protected Builder(final String name, final String user, final String version) {
            super(name, user, version);
        }

        /**
         * The setup file to use with the resource if desired.
         *
         * @param setupFile The setup file location
         * @return The builder
         */
        public T withSetupFile(final String setupFile) {
            this.bSetupFile = setupFile;
            return (T) this;
        }

        /**
         * The configs to use with the resource if desired.
         *
         * @param configs The configuration file locations
         * @return The builder
         */
        public T withConfigs(final Set<String> configs) {
            if (configs != null) {
                this.bConfigs.addAll(configs);
            }
            return (T) this;
        }

        /**
         * Set the dependencies for the entity if desired.
         *
         * @param dependencies The dependencies
         * @return The builder
         */
        public T withDependencies(final Set<String> dependencies) {
            if (dependencies != null) {
                this.bDependencies.addAll(dependencies);
            }
            return (T) this;
        }
    }
}
