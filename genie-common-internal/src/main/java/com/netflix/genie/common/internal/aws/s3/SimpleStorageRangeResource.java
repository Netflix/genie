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
package com.netflix.genie.common.internal.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import io.awspring.cloud.core.io.s3.AmazonS3ProxyFactory;
import io.awspring.cloud.core.io.s3.SimpleStorageResource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.core.task.TaskExecutor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class extends {@link SimpleStorageResource} in order to efficiently handle range requests.
 * Rather than fetching the entire object and let the web tier skip, it only downloads the relevant object region and
 * returns a composite input stream that skips the unrequested bytes.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public final class SimpleStorageRangeResource extends SimpleStorageResource {

    private final AmazonS3 client;
    private final String bucket;
    private final String key;
    private final String versionId;
    private final Pair<Integer, Integer> range;
    private final long contentLength;

    SimpleStorageRangeResource(
        final AmazonS3 client,
        final String bucket,
        final String key,
        final String versionId,
        final TaskExecutor s3TaskExecutor,
        final Pair<Integer, Integer> range
    ) throws IOException {
        super(client, bucket, key, s3TaskExecutor, versionId, null);
        this.client = AmazonS3ProxyFactory.createProxy(client);
        this.bucket = bucket;
        this.key = key;
        this.versionId = versionId;
        this.range = range;

        long tempContentLength = -1;
        try {
            tempContentLength = super.contentLength();
        } catch (FileNotFoundException e) {
            // S3 object does not exist.
            // Upstream code will handle this correctly by checking exists(), contentLength(), etc.
            log.warn("Returning non-existent S3 resource {}/{}", bucket, key);
        }
        this.contentLength = tempContentLength;

        final Integer lower = this.range.getLeft();
        final Integer upper = this.range.getRight();

        if ((lower != null && upper != null && lower > upper)
            || (lower != null && lower > contentLength)) {
            throw new IllegalArgumentException(
                "Invalid range " + lower + "-" + upper + " for S3 object of size " + contentLength
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getInputStream() throws IOException {

        if (!this.exists()) {
            throw new FileNotFoundException("No such object: " + this.bucket + "/" + key);
        }

        // Index of first and last byte to fetch (inclusive)
        final long rangeStart;
        final long rangeEnd;

        if (this.range.getLeft() == null && this.range.getRight() == null) {
            // Full object
            rangeStart = 0;
            rangeEnd = Math.max(0, this.contentLength - 1);
        } else if (this.range.getLeft() == null && this.range.getRight() != null) {
            // Object suffix
            rangeStart = Math.max(0, this.contentLength - this.range.getRight());
            rangeEnd = Math.max(0, this.contentLength - 1);
        } else if (this.range.getLeft() != null && this.range.getRight() == null) {
            // From offset to end
            rangeStart = this.range.getLeft();
            rangeEnd = Math.max(0, this.contentLength - 1);
        } else {
            // Range start and end are provided
            rangeStart = this.range.getLeft();
            rangeEnd = Math.min(this.range.getRight(), this.contentLength - 1);
        }

        log.debug(
            "Resource {}/{} requested range: {}-{}, actual range: {}-{}",
            this.bucket,
            this.key,
            this.range.getLeft(),
            this.range.getRight(),
            rangeStart,
            rangeEnd
        );

        final long skipBytes = Math.max(0, rangeStart);

        final InputStream inputStream;
        if (rangeEnd - rangeStart < 0 || (rangeEnd == 0 && rangeStart == 0)) {
            inputStream = new EmptyInputStream();
        } else {
            final GetObjectRequest getObjectRequest = new GetObjectRequest(this.bucket, this.key)
                .withRange(rangeStart, rangeEnd)
                .withVersionId(this.versionId);
            inputStream = this.client.getObject(getObjectRequest).getObjectContent();
        }

        return new SkipInputStream(skipBytes, inputStream);
    }

    @Override
    public boolean exists() {
        if (this.contentLength == -1) {
            return false;
        }
        return super.exists();
    }

    /**
     * An input stream that skips some amount of bytes because they are ignored by the web tier when sending back
     * the response content.
     */
    private static class SkipInputStream extends InputStream {
        private final InputStream objectRangeInputStream;
        private long skipBytesLeft;

        SkipInputStream(final long bytesToSkip, final InputStream objectRangeInputStream) {
            this.objectRangeInputStream = objectRangeInputStream;
            this.skipBytesLeft = bytesToSkip;
        }

        @Override
        public int read() throws IOException {
            // Overriding other read(...) methods and hoping nobody is using this one directly.
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
            if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException("Invalid read( b[" + b.length + "], " + off + ", " + len + ")");
            }

            // Efficiently skip over range of bytes that should be ignored
            if (this.skipBytesLeft > 0) {
                final long skippedBytesRead = Math.min(this.skipBytesLeft, len);
                this.skipBytesLeft -= skippedBytesRead;
                return Math.toIntExact(skippedBytesRead);
            }

            return this.objectRangeInputStream.read(b, off, len);
        }

        @Override
        public long skip(final long n) throws IOException {
            long skipped = 0;
            if (this.skipBytesLeft > 0) {
                skipped = Math.min(n, this.skipBytesLeft);
                this.skipBytesLeft -= skipped;
            }

            if (skipped < n) {
                skipped += this.objectRangeInputStream.skip(n - skipped);
            }

            return skipped;
        }

        @Override
        public void close() throws IOException {
            super.close();
            this.objectRangeInputStream.close();
        }
    }

    private static class EmptyInputStream extends InputStream {
        @Override
        public int read() throws IOException {
            return -1;
        }
    }
}
