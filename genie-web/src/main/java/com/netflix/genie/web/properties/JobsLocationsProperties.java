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
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.validation.annotation.Validated;

/**
 * Properties for various job related locations.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@Setter
@Validated
public class JobsLocationsProperties {
    @NotEmpty(message = "Archives location is required")
    private String archives = "file:///tmp/genie/archives/";

    @NotEmpty(message = "Attachments temporary location is required")
    private String attachments = "file:///tmp/genie/attachments/";

    @NotEmpty(message = "Jobs dir is required")
    private String jobs = "file:///tmp/genie/jobs/";
}
