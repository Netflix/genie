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
package com.netflix.genie.web.properties;

import com.amazonaws.regions.Regions;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;

/**
 * Properties related to AWS credentials for Genie on top of what Spring Cloud AWS provides.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = "genie.aws.credentials")
@Validated
@Getter
@Setter
public class AwsCredentialsProperties {

    @Nullable
    private String role;

    /**
     * Property bindings for Spring Cloud AWS region specific properties.
     * <p>
     * Spring Cloud AWS doesn't come with properties bindings for their properties. So adding this for completeness.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @ConfigurationProperties(prefix = "cloud.aws.region")
    @Validated
    public static class SpringCloudAwsRegionProperties {

        @Getter
        @Setter
        private boolean auto;

        @Getter
        private Regions region = Regions.US_EAST_1;

        /**
         * Get the region.
         *
         * @return The region
         */
        public String getStatic() {
            return this.region.getName();
        }

        /**
         * Set the static value for the region this instance is running in.
         *
         * @param newStatic The new static region value
         * @throws IllegalArgumentException When {@literal newStatic} can't be parsed by
         *                                  {@link Regions#fromName(String)}
         */
        public void setStatic(final String newStatic) {
            this.region = Regions.fromName(newStatic);
        }
    }
}
