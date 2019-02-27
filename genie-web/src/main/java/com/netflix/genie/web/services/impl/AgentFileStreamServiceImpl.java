/*
 *
 *  Copyright 2019 Netflix, Inc.
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

package com.netflix.genie.web.services.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.genie.common.internal.exceptions.StreamUnavailableException;
import com.netflix.genie.web.services.AgentFileStreamService;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Map;

/**
 * Implementation of {@link AgentFileStreamService}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentFileStreamServiceImpl implements AgentFileStreamService {

    private final Map<@NotBlank String, @NotNull LinkedList<ReadyStream>> parkedStreams = Maps.newHashMap();

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerReadyStream(final ReadyStream stream) {
        @NotBlank final String jobId = stream.getJobId();
        synchronized (this.parkedStreams) {
            if (this.parkedStreams.containsKey(jobId)) {
                this.parkedStreams.get(jobId).add(stream);
            } else {
                final LinkedList<ReadyStream> jobStreamsList = Lists.newLinkedList();
                jobStreamsList.add(stream);
                this.parkedStreams.put(jobId, jobStreamsList);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregisterReadyStream(final ReadyStream readyStream) {
        @NotBlank final String jobId = readyStream.getJobId();
        synchronized (this.parkedStreams) {
            final LinkedList<ReadyStream> jobParkedStreams = this.parkedStreams.get(jobId);
            if (jobParkedStreams != null) {
                jobParkedStreams.remove(readyStream);
                if (jobParkedStreams.isEmpty()) {
                    // Remove empty list
                    this.parkedStreams.remove(jobId);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ActiveStream beginFileStream(
        final @NotBlank String jobId,
        final Path relativePath,
        final long startOffset,
        final long endOffset
    ) throws StreamUnavailableException, IOException {
        synchronized (this.parkedStreams) {
            if (this.parkedStreams.containsKey(jobId)) {
                final LinkedList<ReadyStream> jobParkedStreams = this.parkedStreams.get(jobId);
                while (!jobParkedStreams.isEmpty()) {
                    final ReadyStream stream = jobParkedStreams.remove();
                    try {
                        return stream.activateStream(relativePath, startOffset, endOffset);
                    } catch (StreamUnavailableException e) {
                        log.warn("Parked stream for job {} discarded due to: {}", jobId, e.getMessage(), e);
                    }
                }
                // Remove empty list
                this.parkedStreams.remove(jobId);
            }
        }

        throw new StreamUnavailableException("No streams ready for job id: " + jobId);
    }
}
