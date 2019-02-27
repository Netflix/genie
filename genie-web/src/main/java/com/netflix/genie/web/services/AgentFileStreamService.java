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

package com.netflix.genie.web.services;

import com.netflix.genie.common.internal.exceptions.StreamUnavailableException;
import org.springframework.validation.annotation.Validated;

import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * Service that manages open streams over which the server can request a file to be streamed by the agent.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ThreadSafe
@Validated
public interface AgentFileStreamService {

    /**
     * Register a stream ready for use.
     * This stream handle will be parked in 'ready' state until it's used to request a file or it expires.
     *
     * @param stream the stream
     */
    void registerReadyStream(ReadyStream stream);

    /**
     * Remove a stream from the list of ready to use due to an error or disconnection.
     *
     * @param readyStream the stream
     */
    void unregisterReadyStream(ReadyStream readyStream);

    /**
     * Use one of the parked stream related to the given job to request a file from the agent.
     *
     * @param jobId        the job id
     * @param relativePath the relative path of the file (inside the job directory)
     * @param startOffset  the starting offset of the requested file (inclusive)
     * @param endOffset    the end offset of the requested file (exclusive)
     * @return an {@link ActiveStream}
     * @throws StreamUnavailableException if a stream is not available for this job id
     * @throws IOException                if a stream fails to initialize its buffers during activation
     */
    ActiveStream beginFileStream(
        @NotBlank String jobId,
        Path relativePath,
        long startOffset,
        long endOffset
    ) throws StreamUnavailableException, IOException;

    /**
     * A stream ready to be used to request a file.
     */
    interface ReadyStream extends Closeable {

        /**
         * The id of the job this stream is associated to.
         *
         * @return a job id
         */
        @NotBlank
        String getJobId();

        /**
         * Activate this stream and use it to transfer a file.
         *
         * @param relativePath the relative path of the file to request
         * @param startOffset  the starting offset of the requested file (inclusive)
         * @param endOffset    the end offset of the requested file (exclusive)
         * @return an {@link ActiveStream}
         * @throws StreamUnavailableException if the stream is no longer usable
         * @throws IOException                if an error is encountered initializing the stream
         */
        ActiveStream activateStream(
           Path relativePath,
           long startOffset,
           long endOffset
        ) throws StreamUnavailableException, IOException;
    }

    /**
     * A stream being used to transfer a specific file for a specific job from the agent to the server.
     */
    interface ActiveStream extends Closeable {

        /**
         * Get the relative path of the requested file.
         *
         * @return the file path relative to the job directory root
         */
        @NotNull
        Path getRelativePath();

        /**
         * Get the job id.
         *
         * @return the id of the job this file transfer is associated with
         */
        @NotBlank
        String getJobId();

        /**
         * Get the input stream.
         *
         * @return the input stream
         */
        InputStream getInputStream();

    }
}
