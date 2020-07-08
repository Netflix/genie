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
package com.netflix.genie.agent.properties;

import com.netflix.genie.agent.execution.services.JobMonitorService;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.time.Duration;

/**
 * Properties of {@link JobMonitorService}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@Setter
@Validated
public class JobMonitorServiceProperties {
    /**
     * Interval for checking the job status seen by the server.
     */
    @DurationMin(seconds = 10)
    private Duration checkInterval = Duration.ofMinutes(1);

    /**
     * Maximum number of files in the job directory (files not included in the manifest are exempt).
     */
    @Min(100)
    private int maxFiles = 64_000;

    /**
     * Maximum size of the job directory (files not included in the manifest are exempt).
     */
    @NotNull
    private DataSize maxTotalSize = DataSize.ofGigabytes(16);

    /**
     * Maximum size of any one file in the job directory (files not included in the manifest are exempt).
     */
    @NotNull
    private DataSize maxFileSize = DataSize.ofGigabytes(8);

    /**
     * Whether to monitor the job status seen by the server.
     */
    @NotNull
    private Boolean checkRemoteJobStatus = true;
}
