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
package com.netflix.genie.common.internal.dtos;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * A representation of compute resources that a Genie entity (job/command/etc.) may request or use.
 *
 * @author tgianos
 * @since 4.3.0
 */
public class ComputeResources implements Serializable {

    @Min(value = 1, message = "Must have at least one CPU")
    private final Integer cpu;

    @Min(value = 1, message = "Must have at least one GPU")
    private final Integer gpu;

    @Min(value = 1, message = "Must have at least 1 MB of memory")
    private final Integer memoryMb;

    @Min(value = 1, message = "Must have at least 1 MB of disk space")
    private final Long diskMb;

    @Min(value = 1, message = "Must have at least 1 Mbps of network bandwidth")
    private final Long networkMbps;

    private ComputeResources(final Builder builder) {
        this.cpu = builder.bCpu;
        this.gpu = builder.bGpu;
        this.memoryMb = builder.bMemoryMb;
        this.diskMb = builder.bDiskMb;
        this.networkMbps = builder.bNetworkMbps;
    }

    /**
     * Get the number of CPUs.
     *
     * @return The amount or {@link Optional#empty()}
     */
    public Optional<Integer> getCpu() {
        return Optional.ofNullable(this.cpu);
    }

    /**
     * Get the number of GPUs.
     *
     * @return The amount or {@link Optional#empty()}
     */
    public Optional<Integer> getGpu() {
        return Optional.ofNullable(this.gpu);
    }

    /**
     * Get the amount of memory.
     *
     * @return The amount or {@link Optional#empty()}
     */
    public Optional<Integer> getMemoryMb() {
        return Optional.ofNullable(this.memoryMb);
    }

    /**
     * Get the amount of disk space.
     *
     * @return The amount or {@link Optional#empty()}
     */
    public Optional<Long> getDiskMb() {
        return Optional.ofNullable(this.diskMb);
    }

    /**
     * Get the network bandwidth size.
     *
     * @return The size or {@link Optional#empty()}
     */
    public Optional<Long> getNetworkMbps() {
        return Optional.ofNullable(this.networkMbps);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.cpu, this.gpu, this.memoryMb, this.diskMb, this.networkMbps);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ComputeResources)) {
            return false;
        }
        final ComputeResources that = (ComputeResources) o;
        return Objects.equals(this.cpu, that.cpu)
            && Objects.equals(this.gpu, that.gpu)
            && Objects.equals(this.memoryMb, that.memoryMb)
            && Objects.equals(this.diskMb, that.diskMb)
            && Objects.equals(this.networkMbps, that.networkMbps);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "ComputeResources{"
            + "cpu=" + this.cpu
            + ", gpu=" + this.gpu
            + ", memoryMb=" + this.memoryMb
            + ", diskMb=" + this.diskMb
            + ", networkMbps=" + this.networkMbps
            + '}';
    }

    /**
     * Builder for generating immutable {@link ComputeResources} instances.
     *
     * @author tgianos
     * @since 4.3.0
     */
    public static class Builder {

        private Integer bCpu;
        private Integer bGpu;
        private Integer bMemoryMb;
        private Long bDiskMb;
        private Long bNetworkMbps;

        /**
         * Set the number of CPUs.
         *
         * @param cpu The number must be at least 1 or {@literal null}
         * @return The {@link Builder}
         */
        public Builder withCpu(@Nullable final Integer cpu) {
            this.bCpu = cpu;
            return this;
        }

        /**
         * Set the number of GPUs.
         *
         * @param gpu The number must be at least 1 or {@literal null}
         * @return The {@link Builder}
         */
        public Builder withGpu(@Nullable final Integer gpu) {
            this.bGpu = gpu;
            return this;
        }

        /**
         * Set amount of memory in MB.
         *
         * @param memoryMb The number must be at least 1 or {@literal null}
         * @return The {@link Builder}
         */
        public Builder withMemoryMb(@Nullable final Integer memoryMb) {
            this.bMemoryMb = memoryMb;
            return this;
        }

        /**
         * Set amount of disk space in MB.
         *
         * @param diskMb The number must be at least 1 or {@literal null}
         * @return The {@link Builder}
         */
        public Builder withDiskMb(@Nullable final Long diskMb) {
            this.bDiskMb = diskMb;
            return this;
        }

        /**
         * Set amount of network bandwidth in Mbps.
         *
         * @param networkMbps The number must be at least 1 or {@literal null}
         * @return The {@link Builder}
         */
        public Builder withNetworkMbps(@Nullable final Long networkMbps) {
            this.bNetworkMbps = networkMbps;
            return this;
        }

        /**
         * Create a new immutable {@link ComputeResources} instance based on the current state of this builder instance.
         *
         * @return A {@link ComputeResources} instance
         */
        public ComputeResources build() {
            return new ComputeResources(this);
        }
    }
}
