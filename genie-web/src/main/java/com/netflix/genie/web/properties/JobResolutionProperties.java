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
package com.netflix.genie.web.properties;

import com.netflix.genie.common.internal.dtos.ComputeResources;
import com.netflix.genie.common.internal.dtos.Image;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Properties related to the job resolution process. If loaded as a Spring bean this class with auto refresh its
 * contents.
 *
 * @author tgianos
 * @since 4.3.0
 */
@Validated
public class JobResolutionProperties {

    private static final Logger LOG = LoggerFactory.getLogger(JobResolutionProperties.class);
    private static final String PROPERTY_PREFIX = "genie.services.resolution";
    private static final int MB_TO_MBIT = 8;
    private static final String DEFAULTS_PROPERTY_PREFIX = PROPERTY_PREFIX + ".defaults";
    private static final String RUNTIME_DEFAULTS_PROPERTY_PREFIX = DEFAULTS_PROPERTY_PREFIX + ".runtime";

    private static final Bindable<Runtime> RUNTIME_BINDABLE = Bindable.of(Runtime.class);

    private final Binder binder;
    private ComputeResources defaultComputeResources;
    private Map<String, Image> defaultImages;

    /**
     * Constructor.
     *
     * @param environment The spring environment
     */
    public JobResolutionProperties(final Environment environment) {
        this.binder = Binder.get(environment);
        this.defaultComputeResources = new ComputeResources.Builder().build();
        this.defaultImages = new HashMap<>();
        this.refresh();
    }

    /**
     * Get the default {@link ComputeResources} if any were defined.
     *
     * @return The current default {@link ComputeResources} values
     */
    public ComputeResources getDefaultComputeResources() {
        return this.defaultComputeResources;
    }

    /**
     * Get the default mapping of images.
     *
     * @return The map of image {@literal key} to {@link Image} values
     */
    public Map<String, Image> getDefaultImages() {
        return this.defaultImages;
    }

    /**
     * Refresh the values of the properties contained within this object.
     */
    @Scheduled(fixedRate = 30L, timeUnit = TimeUnit.SECONDS)
    public void refresh() {
        LOG.debug("Refreshing job resolution properties");
        final Runtime defaultRuntime = this.binder.bindOrCreate(RUNTIME_DEFAULTS_PROPERTY_PREFIX, RUNTIME_BINDABLE);
        final Resources resources = defaultRuntime.getResources();
        this.defaultComputeResources = new ComputeResources.Builder()
            .withCpu(resources.getCpu())
            .withGpu(resources.getGpu())
            .withMemoryMb(resources.getMemory().toMegabytes())
            .withDiskMb(resources.getDisk().toMegabytes())
            .withNetworkMbps(resources.getNetwork().toMegabytes() * MB_TO_MBIT)
            .build();
        this.defaultImages = defaultRuntime
            .getImages()
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                        final DefaultImage defaultImage = entry.getValue();
                        final Image.Builder builder = new Image.Builder();
                        defaultImage.getName().ifPresent(builder::withName);
                        defaultImage.getTag().ifPresent(builder::withTag);
                        builder.withArguments(defaultImage.getArguments());
                        return builder.build();
                    }
                )
            );
        LOG.debug(
            "Completed refresh of job resolution properties. New resource values = {}, new image values = {}",
            this.defaultComputeResources,
            this.defaultImages
        );
    }

    /**
     * Container for the runtime defaults set in properties.
     */
    public static class Runtime {
        private Resources resources;
        private final Map<String, DefaultImage> images;

        /**
         * Constructor.
         */
        public Runtime() {
            this.resources = new Resources();
            this.images = new HashMap<>();
        }

        /**
         * Get the {@link Resources}.
         *
         * @return The resources defined by default
         */
        public Resources getResources() {
            return this.resources;
        }

        /**
         * Set the new resources or fall back to default.
         *
         * @param resources The new resource or {@literal null} to reset to default.
         */
        public void setResources(@Nullable final Resources resources) {
            this.resources = resources == null ? new Resources() : resources;
        }

        /**
         * The map of image key to image definitions.
         *
         * @return The images
         */
        public Map<String, DefaultImage> getImages() {
            return images;
        }

        /**
         * Set the new default image mappings.
         *
         * @param images The new mappings or if {@literal null} empty the set of mappings
         */
        public void setImages(@Nullable final Map<String, DefaultImage> images) {
            this.images.clear();
            if (images != null) {
                this.images.putAll(images);
            }
        }
    }

    /**
     * Computation resource properties.
     */
    public static class Resources {
        private static final int DEFAULT_CPU = 1;
        private static final int DEFAULT_GPU = 0;
        private static final DataSize DEFAULT_MEMORY = DataSize.ofMegabytes(1_500L);
        private static final DataSize DEFAULT_DISK = DataSize.ofGigabytes(10L);
        private static final DataSize DEFAULT_NETWORK = DataSize.ofMegabytes(1_250L); // 10000 mbps

        /**
         * The default number of CPUs that should be allocated to a job if no other value was requested. Min 1.
         */
        @Min(1)
        private int cpu = DEFAULT_CPU;

        /**
         * The default number of GPUs that should be allocated to a job if no other value was requested. Min 0.
         */
        @Min(0)
        private int gpu = DEFAULT_GPU;

        /**
         * The amount of memory that should be allocated to a job if no other value was requested.
         */
        @NotNull
        private DataSize memory = DEFAULT_MEMORY;

        /**
         * The amount of disk space that should be allocated to a job if no other value was requested.
         */
        @NotNull
        private DataSize disk = DEFAULT_DISK;

        /**
         * The amount of network bandwidth that should be allocated to a job if no other value was requested. Will be
         * converted to Mbps.
         */
        @NotNull
        private DataSize network = DEFAULT_NETWORK;

        /**
         * Get the default number of CPUs.
         *
         * @return The default number of CPUs
         */
        public int getCpu() {
            return this.cpu;
        }

        /**
         * Set the new default CPU value.
         *
         * @param cpu The cpu value or {@literal null}
         */
        public void setCpu(@Min(1) @Nullable final Integer cpu) {
            this.cpu = cpu == null ? DEFAULT_CPU : cpu;
        }

        /**
         * Get the number of GPUs.
         *
         * @return The number of GPUs.
         */
        public int getGpu() {
            return this.gpu;
        }

        /**
         * Set the new default amount of GPUs.
         *
         * @param gpu The new number of GPUs or {@literal null} to set back to default
         */
        public void setGpu(@Min(0) @Nullable final Integer gpu) {
            this.gpu = gpu == null ? DEFAULT_GPU : gpu;
        }

        /**
         * Get the default amount of memory.
         *
         * @return The amount of memory as a {@link DataSize} instance
         */
        public DataSize getMemory() {
            return this.memory;
        }

        /**
         * Set the default amount of memory for the job runtime.
         *
         * @param memory The new amount of memory or {@literal null} to reset to default
         */
        public void setMemory(@Nullable final DataSize memory) {
            this.memory = memory == null ? DEFAULT_MEMORY : memory;
        }

        /**
         * Get the amount of disk space to allocate for the job.
         *
         * @return The disk space as {@link DataSize} instance
         */
        public DataSize getDisk() {
            return this.disk;
        }

        /**
         * Set the new disk size.
         *
         * @param disk The new disk size or {@literal null} to set back to default
         */
        public void setDisk(@Nullable final DataSize disk) {
            this.disk = disk == null ? DEFAULT_DISK : disk;
        }

        /**
         * Get the network bandwidth that should be allocated for the job.
         *
         * @return The network bandwidth as a {@link DataSize} instance that should be converted to something like
         * {@literal Mbps}
         */
        public DataSize getNetwork() {
            return this.network;
        }

        /**
         * Set the new network bandwidth default for jobs.
         *
         * @param network The new default or if {@literal null} revert to hardcoded default
         */
        public void setNetwork(@Nullable final DataSize network) {
            this.network = network == null ? DEFAULT_NETWORK : network;
        }
    }

    /**
     * Defaults for container images that will combine together to execute the Genie job.
     */
    public static class DefaultImage {
        private String name;
        private String tag;
        private final List<String> arguments;

        /**
         * Constructor.
         */
        public DefaultImage() {
            this.arguments = new ArrayList<>();
        }

        /**
         * Get the name of the image if one was set.
         *
         * @return The name or {@link Optional#empty()}
         */
        public Optional<String> getName() {
            return Optional.ofNullable(this.name);
        }

        /**
         * Set the new name of the image.
         *
         * @param name The name or {@literal null}
         */
        public void setName(@Nullable final String name) {
            this.name = name;
        }

        /**
         * Get the tag for the image that should be used if one was set.
         *
         * @return The tag or {@link Optional#empty()}
         */
        public Optional<String> getTag() {
            return Optional.ofNullable(this.tag);
        }

        /**
         * Set the new tag for the image.
         *
         * @param tag The tag or {@literal null}
         */
        public void setTag(@Nullable final String tag) {
            this.tag = tag;
        }

        /**
         * Get the list of arguments that should be used when launching the image.
         *
         * @return The list of arguments as unmodifiable list. Attempts to modify will throw exception.
         */
        public List<String> getArguments() {
            return Collections.unmodifiableList(this.arguments);
        }

        /**
         * Set the arguments for the image.
         *
         * @param arguments The new arguments. {@literal null} will set the arguments to an empty list.
         */
        public void setArguments(@Nullable final List<String> arguments) {
            this.arguments.clear();
            if (arguments != null) {
                this.arguments.addAll(arguments);
            }
        }
    }
}
