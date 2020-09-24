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

import javax.validation.constraints.NotNull;
import java.net.URI;

/**
 * Properties for various job related locations.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConfigurationProperties(prefix = JobsLocationsProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class JobsLocationsProperties {

    /**
     * The property prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "genie.jobs.locations";

    private static final String SYSTEM_TMP_DIR = System.getProperty("java.io.tmpdir", "/tmp/");

    @NotNull(message = "Archives storage location is required")
    private URI archives = URI.create("file://" + SYSTEM_TMP_DIR + "genie/archives/");

    @Deprecated
    @NotNull(message = "Default job working directory is required")
    private URI jobs = URI.create("file://" + SYSTEM_TMP_DIR + "genie/jobs/");
}
