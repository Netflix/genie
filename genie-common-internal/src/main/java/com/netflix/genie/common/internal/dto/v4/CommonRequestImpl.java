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

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Size;
import java.util.Optional;

/**
 * Common fields for all Genie 4 resource creation requests (JobRequest, ClusterRequest, etc).
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
abstract class CommonRequestImpl implements CommonRequest {
    @Size(max = 255, message = "Max length for the ID is 255 characters")
    private final String requestedId;
    @Valid
    private final ExecutionEnvironment resources;

    /**
     * Constructor.
     *
     * @param builder The builder to get values from
     */
    CommonRequestImpl(final Builder builder) {
        this(builder.bRequestedId, builder.bResources);
    }

    /**
     * Constructor.
     *
     * @param requestedId The id requested by the user. Optional.
     * @param resources   The execution environment resources requested by the user. Optional.
     */
    CommonRequestImpl(@Nullable final String requestedId, @Nullable final ExecutionEnvironment resources) {
        this.requestedId = requestedId;
        this.resources = resources == null
            ? new ExecutionEnvironment(null, null, null)
            : resources;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getRequestedId() {
        return Optional.ofNullable(this.requestedId);
    }

    /**
     * Builder for common request fields.
     *
     * @param <T> Type of builder that extends this
     * @author tgianos
     * @since 4.0.0
     */
    // NOTE: These abstract class builders are marked public not protected due to a JDK bug from 1999 which caused
    //       issues with Clojure clients which use reflection to make the Java API calls.
    //       http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4283544
    //       Setting them to public seems to have solved the issue at the expense of "proper" code design
    @SuppressWarnings("unchecked")
    @Getter(AccessLevel.PACKAGE)
    public abstract static class Builder<T extends Builder> {
        private String bRequestedId;
        private ExecutionEnvironment bResources;

        /**
         * Constructor.
         */
        Builder() {
        }

        /**
         * Set the id being requested for the resource. Will be rejected if the ID is already used by another resource
         * of the same type. If not included a GUID will be supplied.
         *
         * @param requestedId The requested id. Max of 255 characters.
         * @return The builder
         */
        public T withRequestedId(@Nullable final String requestedId) {
            this.bRequestedId = StringUtils.isBlank(requestedId) ? null : requestedId;
            return (T) this;
        }

        /**
         * Set the execution resources for this resource. e.g. setup file or configuration files etc
         *
         * @param resources The resources to use
         * @return The builder
         */
        public T withResources(@Nullable final ExecutionEnvironment resources) {
            this.bResources = resources;
            return (T) this;
        }
    }
}
