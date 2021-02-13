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
package com.netflix.genie.common.internal.properties;

import com.netflix.genie.common.internal.util.ExponentialBackOffTrigger;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Duration;

/**
 * Properties for {@link com.netflix.genie.common.internal.util.ExponentialBackOffTrigger} used in various places.
 * <p>
 * Notice this class is not tagged as {@link org.springframework.boot.context.properties.ConfigurationProperties}
 * since it's not a root property class.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ExponentialBackOffTriggerProperties {
    private ExponentialBackOffTrigger.DelayType delayType =
        ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_COMPLETION;
    private Duration minDelay = Duration.ofMillis(100);
    private Duration maxDelay = Duration.ofSeconds(3);
    private float factor = 1.2f;
}
