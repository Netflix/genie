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
package com.netflix.genie.web.util;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A temporary in-memory structure to hold in-transit data.
 * Provides an {@code InputStream} for reading, reading blocks until data becomes available or the buffer is closed.
 * <p>
 * To avoid in-memory data growing excessively, this buffer stores a single "chunk" at the time.
 * Only after a chunk is consumed, a new one can be appended.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ThreadSafe
@Slf4j
public class StreamBuffer {

    private final Object lock = new Object();
    private final AtomicReference<StreamBufferInputStream> inputStreamRef = new AtomicReference<>();

    private boolean closed;
    private ByteString currentChunk;
    private int currentChunkWatermark;

    /**
     * Constructor.
     */
    public StreamBuffer() {
        this.inputStreamRef.set(new StreamBufferInputStream(this));
    }

    /**
     * Close this buffer before all data is written due to an error.
     * Reading will return the end of stream marker after the current chunk (if any) has been consumed.
     *
     * @param t the cause for the buffer to be closed.
     */
    public void closeForError(final Throwable t) {
        log.error("Closing buffer due to error: " + t.getClass().getSimpleName() + ": " + t.getMessage());
        this.closeForCompleted();
    }

    /**
     * Close this buffer because all expected data has been written
     * Reading will return the end of stream marker after all data has been consumed.
     */
    public void closeForCompleted() {
        synchronized (this.lock) {
            this.closed = true;
            this.lock.notifyAll();
        }
    }

    /**
     * Append a chunk of data for consumption.
     * This call may block and not return until some data is read/consumed.
     *
     * @param data the data to write into the buffer
     * @throws IllegalStateException if writing is attempted after the buffer has been closed
     */
    public void write(final ByteString data) {
        synchronized (this.lock) {
            while (true) {
                if (this.closed) {
                    throw new IllegalStateException("Attempting to write after closing");
                } else if (this.currentChunk == null) {
                    // Save this chunk so it can be consumed
                    this.currentChunk = data;
                    this.currentChunkWatermark = 0;
                    // Wake up reading thread
                    this.lock.notifyAll();
                    return;
                } else {
                    // Previous chunk of data is still being consumed. Wait.
                    try {
                        this.lock.wait();
                    } catch (InterruptedException e) {
                        log.warn("Interrupted while waiting to write next chunk of data");
                    }
                }
            }
        }
    }

    /**
     * Obtain the input stream to read this data.
     *
     * @return the input stream
     * @throws IllegalStateException if invoked multiple times
     */
    public InputStream getInputStream() {
        final InputStream inputStream = this.inputStreamRef.getAndSet(null);
        if (inputStream == null) {
            throw new IllegalStateException("Input stream for this buffer is no longer available");
        }
        return inputStream;
    }

    private int read(final byte[] destination) {
        synchronized (this.lock) {
            while (true) {
                if (currentChunk != null) {
                    // Read from current chunk into destination
                    final int leftInCurrentChunk = this.currentChunk.size() - this.currentChunkWatermark;
                    final int bytesRead = Math.min(leftInCurrentChunk, destination.length);
                    this.currentChunk.copyTo(destination, currentChunkWatermark, 0, bytesRead);

                    // Update watermark
                    this.currentChunkWatermark += bytesRead;

                    // Is chunk completely consumed?
                    if (this.currentChunkWatermark == this.currentChunk.size()) {
                        // Make room for the next one
                        this.currentChunk = null;
                        // Wake the writer thread
                        this.lock.notifyAll();
                    }
                    return bytesRead;
                } else if (this.closed) {
                    // There won't be another chunk appended
                    return -1;
                } else {
                    try {
                        this.lock.wait();
                    } catch (InterruptedException e) {
                        log.warn("Interrupted while attempting read");
                        return 0;
                    }
                }
            }
        }
    }

    private static class StreamBufferInputStream extends InputStream {
        private final StreamBuffer streamBuffer;

        StreamBufferInputStream(final StreamBuffer streamBuffer) {
            this.streamBuffer = streamBuffer;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int read() {
            // Overriding other read() methods and hoping nobody is referring to this one directly.
            throw new NotImplementedException("Not implemented");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {

            if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException("Invalid read( b[" + b.length + "], " + off + ", " + len + ")");
            }

            final byte[] temporary = new byte[len];

            final int bytesRead = this.streamBuffer.read(temporary);

            if (bytesRead > 0) {
                System.arraycopy(temporary, 0, b, off, bytesRead);
            }

            return bytesRead;
        }
    }

}
