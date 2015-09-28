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

import io.swagger.annotations.ApiModelProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class DTO for configuration service DTOs like Application, Cluster, Command.
 *
 * @author tgianos
 * @since 3.0.0
 */
public abstract class ConfigDTO extends BaseDTO {

    @ApiModelProperty(
            value = "Locations of all the configuration files needed for this resource"
    )
    private final Set<String> configs = new HashSet<>();

    /**
     * Constructor.
     *
     * @param builder The builder to use
     */
    protected ConfigDTO(final Builder builder) {
        super(builder);
        if (builder.bConfigs != null) {
            this.configs.addAll(builder.bConfigs);
        }
    }

    /**
     * Get the configuration files for this config resource.
     *
     * @return The configuration file locations as a read-only set. Any attempt to modify will throw exception.
     */
    public Set<String> getConfigs() {
        return Collections.unmodifiableSet(this.configs);
    }

    /**
     * A builder for helping to create instances.
     *
     * @param <T> The type of builder that extends this builder for final implementation
     * @author tgianos
     * @since 3.0.0
     */
    protected abstract static class Builder<T extends Builder> extends BaseDTO.Builder<T> {

        private Set<String> bConfigs = new HashSet<>();

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
    }
}
