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
import java.util.Optional;

/**
 * Base class DTO for DTOs which require a setup file.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
public abstract class SetupFileDTO extends CommonDTO {

    private static final long serialVersionUID = 2116254045303538065L;
    @Size(max = 1024, message = "Max length of the setup file is 1024 characters")
    private final String setupFile;

    /**
     * Constructor.
     *
     * @param builder The builder to use
     */
    @SuppressWarnings("unchecked")
    protected SetupFileDTO(final Builder builder) {
        super(builder);
        this.setupFile = builder.bSetupFile;
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
     * A builder for helping to create instances.
     *
     * @param <T> The type of builder that extends this builder for final implementation
     * @author tgianos
     * @since 3.0.0
     */
    @SuppressWarnings("unchecked")
    protected abstract static class Builder<T extends Builder> extends CommonDTO.Builder<T> {

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
    }
}
