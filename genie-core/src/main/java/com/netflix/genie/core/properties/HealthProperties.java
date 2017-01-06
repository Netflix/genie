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
package com.netflix.genie.core.properties;

import lombok.Getter;
import lombok.Setter;

/**
 * All properties related to health thresholds in Genie.
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Getter
@Setter
public class HealthProperties {
    /**
     * Defines the threshold for the maximum CPU load percentage. Health of the system is marked OUT_OF_SERVICE if
     * the CPU load of a system goes beyond this threshold for <code>maxCpuLoadConsecutiveOccurrences</code>
     * consecutive times.
     * Default to 80 percentage.
     */
    private double maxCpuLoadPercent = 80;
    /**
     * Defines the threshold of consecutive occurrences of CPU load crossing the <code>maxCpuLoadPercent</code>.
     * Health of the system is marked OUT_OF_SERVICE if the CPU load of a system goes beyond the threshold
     * <code>maxCpuLoadPercent</code> for <code>maxCpuLoadConsecutiveOccurrences</code> consecutive times.
     * Default to 3.
     */
    private int maxCpuLoadConsecutiveOccurrences = 3;
}
