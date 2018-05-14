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

package com.netflix.genie.core.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;

/**
 * Properties to configure an ExponentialBackOffTrigger.
 *
 * @author mprimi
 * @since 3.3.9
 */

@Getter
@Setter
@Validated
public class ExponentialBackOffTriggerProperties {
    @Min(value = 1)
    private long minInterval = 100;
    private long maxInterval = 10_000;
    private float factor = 1.2f;
}
