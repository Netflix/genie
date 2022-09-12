/*
 *
 *  Copyright 2022 Netflix, Inc.
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
package com.netflix.genie.common.internal.dtos

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link ComputeResources}.
 *
 * @author tgianos
 */
class ComputeResourcesSpec extends Specification {

    @Unroll
    def "can validate for #cpu, #gpu, #memoryMb, #diskMb, #networkMbps"() {
        when:
        def computeResources0 = new ComputeResources.Builder()
            .withCpu(cpu)
            .withGpu(gpu)
            .withMemoryMb(memoryMb)
            .withDiskMb(diskMb)
            .withNetworkMbps(networkMbps)
            .build()

        def computeResources1 = new ComputeResources.Builder()
            .withCpu(cpu)
            .withGpu(gpu)
            .withMemoryMb(memoryMb)
            .withDiskMb(diskMb)
            .withNetworkMbps(networkMbps)
            .build()

        def computeResources2 = new ComputeResources.Builder()
            .withCpu(cpu == null ? 1 : cpu + 1)
            .withGpu(gpu == null ? 1 : gpu + 1)
            .withMemoryMb(memoryMb == null ? 1 : memoryMb + 1)
            .withDiskMb(diskMb == null ? 1L : diskMb + 1L)
            .withNetworkMbps(networkMbps == null ? 1L : networkMbps + 1L)
            .build()

        then:
        computeResources0.getCpu() == Optional.ofNullable(cpu)
        computeResources0.getGpu() == Optional.ofNullable(gpu)
        computeResources0.getMemoryMb() == Optional.ofNullable(memoryMb)
        computeResources0.getDiskMb() == Optional.ofNullable(diskMb)
        computeResources0.getNetworkMbps() == Optional.ofNullable(networkMbps)

        computeResources0 == computeResources1
        computeResources0 != computeResources2

        computeResources0.hashCode() == computeResources1.hashCode()
        computeResources0.hashCode() != computeResources2.hashCode()

        computeResources0.toString() == computeResources1.toString()
        computeResources0.toString() != computeResources2.toString()

        where:
        cpu  | gpu  | memoryMb | diskMb | networkMbps
        1    | 2    | 3        | 4L     | 5L
        1    | null | null     | null   | null
        null | 2    | null     | null   | null
        null | null | 3        | null   | null
        null | null | null     | 4L     | null
        null | null | null     | null   | 5L
        null | null | null     | null   | null
    }
}
