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

package com.netflix.genie.common.internal.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory of {@link FileBuffer} which provides its own implementation of the interface.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class FileBufferFactory {

    private static final String TEMP_FILE_PREFIX = FileBuffer.class.getSimpleName() + "-";
    private static final String TEMP_FILE_SUFFIX = null;

    /**
     * Get a new {@link FileBuffer}.
     *
     * @param size the exact size the file buffer is expected to eventually reach
     * @return a new {@link FileBuffer}
     * @throws IOException in case of error creating the file buffer
     */
    public FileBuffer get(final int size) throws IOException {
        final Path tempFilePath = Files.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        return new FileBufferImpl(tempFilePath, size);
    }

    private static class FileBufferImpl implements FileBuffer {
        private static final OpenOption[] FILE_READ_OPTIONS = {
            StandardOpenOption.READ,
            StandardOpenOption.DELETE_ON_CLOSE,
        };
        private static final OpenOption[] FILE_WRITE_OPTIONS = {
            StandardOpenOption.WRITE,
        };
        private final Path tempFilePath;
        private final OutputStream outputStream;
        private final InputStream inputStream;

        FileBufferImpl(final Path tempFilePath, final int expectedFileSize) throws IOException {
            this.tempFilePath = tempFilePath;
            this.outputStream = new FileBufferOutputStream(this.tempFilePath, expectedFileSize);
            this.inputStream = new FileBufferInputStream(this.tempFilePath, expectedFileSize);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public InputStream getInputStream() {
            return this.inputStream;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public OutputStream getOutputStream() {
            return this.outputStream;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Path getTemporaryFilePath() {
            return this.tempFilePath;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            this.inputStream.close();
            this.outputStream.close();
        }

        /**
         * Implementation of InputStream that relies on the pre-determined file size.
         * - Blocks read until more data is available
         * - Returns -1 once the entire file content has been read
         */
        private static class FileBufferInputStream extends InputStream {
            private final InputStream fileInputStream;
            private final int expectedFileSize;
            private AtomicBoolean closed = new AtomicBoolean(false);
            private AtomicInteger watermark = new AtomicInteger(0);

            FileBufferInputStream(final Path tempFilePath, final int expectedFileSize) throws IOException {
                this.fileInputStream = Files.newInputStream(tempFilePath,  FILE_READ_OPTIONS);
                this.expectedFileSize = expectedFileSize;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int read() {
                throw new NotImplementedException("Not supported");
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int read(@Nonnull final byte[] b) throws IOException {
                return this.read(b, 0, b.length);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int read(@Nonnull final byte[] b, final int off, final int len) throws IOException {
                if (this.isDrained()) {
                    return -1;
                }
                this.waitForData(1);
                final int bytesRead = this.fileInputStream.read(b, off, len);
                this.watermark.getAndAdd(bytesRead);
                return bytesRead;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public long skip(final long n) throws IOException {
                throw new IOException("Skip not supported");
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int available() throws IOException {
                throwIfClosed();
                return this.fileInputStream.available();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void close() throws IOException {
                if (!this.closed.getAndSet(true)) {
                    this.fileInputStream.close();
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public synchronized void reset() throws IOException {
                super.reset();
            }

            private void throwIfClosed() throws IOException {
                if (closed.get()) {
                    throw new IOException("Stream closed");
                }
            }

            private void waitForData(final int minimumBytes) throws IOException {
                // Wait until file grows to at  least: watermark + sizeof(int)
                while (this.available() < 1) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting for data", e);
                    }
                    throwIfClosed();
                }
            }

            private boolean isDrained() {
                return this.watermark.get() == this.expectedFileSize;
            }
        }

        /**
         * Implementation of OutputStream that uses a FileOutputStream and relies on the pre-determined file size to
         * avoid writing past the expected file size.
         */
        private static class FileBufferOutputStream extends OutputStream {
            private final OutputStream fileOutputStream;
            private final int expectedFileSize;
            private final AtomicInteger bytesWritten = new AtomicInteger();
            private final AtomicBoolean closed = new AtomicBoolean();

            FileBufferOutputStream(final Path tempFilePath, final int expectedFileSize) throws IOException {
                this.fileOutputStream = Files.newOutputStream(tempFilePath, FileBufferImpl.FILE_WRITE_OPTIONS);
                this.expectedFileSize = expectedFileSize;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void write(final int b) throws IOException {
                throw new NotImplementedException("Not supported");
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void write(final byte[] b) throws IOException {
                this.write(b, 0, b.length);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                throwIfClosed();
                final int newBytesWritten = this.bytesWritten.addAndGet(len);
                if (newBytesWritten > expectedFileSize) {
                    throw new ArrayIndexOutOfBoundsException("Wrote data past the expected size");
                }
                this.fileOutputStream.write(b, off, len);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void flush() throws IOException {
                throwIfClosed();
                this.fileOutputStream.flush();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void close() throws IOException {
                if (!this.closed.getAndSet(true)) {
                    this.fileOutputStream.close();
                }
            }

            private void throwIfClosed() throws IOException {
                if (closed.get()) {
                    throw new IOException("Stream closed");
                }
            }
        }
    }
}
