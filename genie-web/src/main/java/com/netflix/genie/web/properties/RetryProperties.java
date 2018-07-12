/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * All properties related to Http retry template in Genie.
 *
 * @author amajumdar
 * @since 3.0.0
 */
@ConfigurationProperties(prefix = RetryProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class RetryProperties {

    /**
     * The property prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "genie.retry";

    /**
     * Total number of retries to attempt.
     * Default to 5 retries.
     */
    @Min(1)
    private int noOfRetries = 5;

    /**
     * Default to 10000 ms.
     */
    @Min(1L)
    private long initialInterval = 10_000L;

    /**
     * Defaults to 60000 ms.
     */
    @Min(1L)
    @Max(Long.MAX_VALUE)
    private long maxInterval = 60_000L;

    @Valid
    private S3RetryProperties s3 = new S3RetryProperties();

    /**
     * Retry properties related to S3.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Validated
    @Getter
    @Setter
    public static class S3RetryProperties {

        @Min(1)
        private int noOfRetries = 5;
    }
}
