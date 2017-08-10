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
package com.netflix.genie.core.services.impl;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Class to test the S3FileTransferImpl class.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class S3FileTransferImplUnitTests {

    private static final String S3_PREFIX = "s3://";
    private static final String S3_BUCKET = "bucket";
    private static final String S3_KEY = "key";
    private static final String S3_PATH = S3_PREFIX + S3_BUCKET + "/" + S3_KEY;
    private static final String LOCAL_PATH = "local";

    private S3FileTransferImpl s3FileTransfer;
    private AmazonS3Client s3Client;
    private Timer downloadTimer;
    private Timer uploadTimer;

    /**
     * Setup the tests.
     *
     * @throws GenieException If there is a problem.
     */
    @Before
    public void setup() throws GenieException {
        final Registry registry = Mockito.mock(Registry.class);
        this.downloadTimer = Mockito.mock(Timer.class);
        Mockito.when(registry.timer("genie.files.s3.download.timer")).thenReturn(this.downloadTimer);
        this.uploadTimer = Mockito.mock(Timer.class);
        Mockito.when(registry.timer("genie.files.s3.upload.timer")).thenReturn(this.uploadTimer);
        this.s3Client = Mockito.mock(AmazonS3Client.class);
        this.s3FileTransfer = new S3FileTransferImpl(this.s3Client, registry);
    }

    /**
     * Given a set of valid S3 {prefix,bucket,key}, try all combinations.
     * Ensure they are accepted as valid and the path components are parsed correctly.
     * @throws GenieException in case of error building the URI
     */
    @Test
    public void testValidS3Paths() throws GenieException {
        final String[] prefixes = {
            "s3://",
            "s3n://",
        };

        final String[] buckets = {
            "bucket",
            "bucket1",
            "1bucket",
            "bucket-bucket",
            "bucket.bucket",
        };

        final String[] keys = {
            "Development/Projects1.xls",
            "Finance/statement1.pdf",
            "Private/taxdocument.pdf",
            "s3-dg.pdf",
            "weird/but/valid/key!-_*'().pdf",
        };

        for (final String prefix : prefixes) {
            for (final String bucket : buckets) {
                for (final String key : keys) {
                    final String path = prefix + bucket + "/" + key;
                    Assert.assertTrue(this.s3FileTransfer.isValid(path));
                    final AmazonS3URI s3Uri = this.s3FileTransfer.getS3Uri(path);
                    Assert.assertEquals(bucket, s3Uri.getBucket());
                    Assert.assertEquals(key, s3Uri.getKey());
                }
            }
        }
    }

    /**
     * Verify invalid S3 prefixes are rejected.
     * @throws GenieException in case of unexpected validation error
     */
    @Test
    public void testInvalidS3Prefixes() throws GenieException {
        final String[] invalidPrefixes = {
            "",
            " ",
            "s3/",
            "s3//",
            "s3:/",
            "s3x:/",
            "foo://",
            "file://",
            "http://",
        };

        for (final String invalidPrefix : invalidPrefixes) {
            final String path = invalidPrefix + "bucket/key";
            Assert.assertFalse(this.s3FileTransfer.isValid(path));
            boolean genieException = false;
            try {
                this.s3FileTransfer.getS3Uri(path);
            } catch (GenieServerException e) {
                genieException = true;
            } finally {
                Assert.assertTrue(genieException);
            }
        }
    }

    /**
     * Verify invalid S3 bucket names are rejected.
     * @throws GenieException in case of unexpected validation error
     */
    @Test
    public void testInvalidS3Buckets() throws GenieException {
        final String[] invalidBuckets = {
            "",
            " ",
            "a",
            "aa",
            "Bucket",
            ".bucket",
            "bucket.",
            "buc:ket",
            "buc!ket",
            "buc[ket",
            "buc(ket",
            "buc'ket",
            "buc ket",
            StringUtils.leftPad("", 64, "b"),
            ".",
            "/",
            // "buc..ket", // Invalid, but current logic does not catch this
        };

        for (final String invalidBucket : invalidBuckets) {
            final String path = "s3://" + invalidBucket + "/key";
            Assert.assertFalse(this.s3FileTransfer.isValid(path));
            boolean genieException = false;
            try {
                this.s3FileTransfer.getS3Uri(path);
            } catch (GenieServerException e) {
                genieException = true;
            } finally {
                Assert.assertTrue(genieException);
            }
        }
    }

    /**
     * Verify invalid S3 key names are rejected.
     * @throws GenieException in case of unexpected validation error
     */
    @Test
    public void testInvalidS3Keys() throws GenieException {
        final String[] invalidKeys = {
            "",
            " ",
            "/",
            "//",
            "[key]",
        };

        for (final String invalidKey : invalidKeys) {
            final String path = "s3://bucket/" + invalidKey;
            Assert.assertFalse(this.s3FileTransfer.isValid(path));
            boolean genieException = false;
            try {
                this.s3FileTransfer.getS3Uri(path);
            } catch (GenieServerException e) {
                genieException = true;
            } finally {
                Assert.assertTrue(genieException);
            }
        }
    }

    /**
     * Test the getFile method for invalid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testGetFileMethodInvalidS3Path() throws GenieException {
        final String invalidS3Path = "filepath";
        try {
            s3FileTransfer.getFile(invalidS3Path, LOCAL_PATH);
        } finally {
            Mockito
                .verify(this.downloadTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        }
    }

    /**
     * Test the getFile method for valid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testGetFileMethodValidS3Path() throws GenieException {

        final ObjectMetadata objectMetadata = Mockito.mock(ObjectMetadata.class);
        Mockito.when(this.s3Client.getObject(Mockito.any(GetObjectRequest.class), Mockito.any(File.class)))
            .thenReturn(objectMetadata);
        final ArgumentCaptor<GetObjectRequest> argument = ArgumentCaptor.forClass(GetObjectRequest.class);

        s3FileTransfer.getFile(S3_PATH, LOCAL_PATH);
        Mockito.verify(this.s3Client).getObject(argument.capture(), Mockito.any());
        Assert.assertEquals(S3_BUCKET, argument.getValue().getBucketName());
        Assert.assertEquals(S3_KEY, argument.getValue().getKey());
        Mockito
            .verify(this.downloadTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Test the getFile method for valid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testGetFileMethodFailureToFetch() throws GenieException {
        Mockito.when(this.s3Client.getObject(Mockito.any(GetObjectRequest.class), Mockito.any(File.class)))
            .thenThrow(new AmazonS3Exception("something"));
        final ArgumentCaptor<GetObjectRequest> argument = ArgumentCaptor.forClass(GetObjectRequest.class);

        try {
            s3FileTransfer.getFile(S3_PATH, LOCAL_PATH);
        } finally {
            Mockito.verify(this.s3Client).getObject(argument.capture(), Mockito.any());
            Assert.assertEquals(S3_BUCKET, argument.getValue().getBucketName());
            Assert.assertEquals(S3_KEY, argument.getValue().getKey());
            Mockito
                .verify(this.downloadTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        }
    }

    /**
     * Test the putFile method for invalid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testPutFileMethodInvalidS3Path() throws GenieException {
        final String invalidS3Path = "filepath";
        try {
            s3FileTransfer.putFile(LOCAL_PATH, invalidS3Path);
        } finally {
            Mockito
                .verify(this.uploadTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        }
    }

    /**
     * Test the putFile method for valid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testPutFileMethodValidS3Path() throws GenieException {

        final PutObjectResult putObjectResult = Mockito.mock(PutObjectResult.class);
        Mockito.when(this.s3Client.putObject(Mockito.any(), Mockito.any(), Mockito.any(File.class)))
            .thenReturn(putObjectResult);
        final ArgumentCaptor<String> bucketArgument = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> keyArgument = ArgumentCaptor.forClass(String.class);

        s3FileTransfer.putFile(LOCAL_PATH, S3_PATH);
        Mockito
            .verify(this.s3Client)
            .putObject(bucketArgument.capture(), keyArgument.capture(), Mockito.any(File.class));
        Assert.assertEquals(S3_BUCKET, bucketArgument.getValue());
        Assert.assertEquals(S3_KEY, keyArgument.getValue());
        Mockito
            .verify(this.uploadTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Test the putFile method for valid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testPutFileMethodFailureToFetch() throws GenieException {
        Mockito
            .when(this.s3Client.putObject(Mockito.any(), Mockito.any(), Mockito.any(File.class)))
            .thenThrow(new AmazonS3Exception("something"));
        final ArgumentCaptor<String> bucketArgument = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> keyArgument = ArgumentCaptor.forClass(String.class);

        try {
            s3FileTransfer.putFile(LOCAL_PATH, S3_PATH);
        } finally {
            Mockito
                .verify(this.s3Client)
                .putObject(bucketArgument.capture(), keyArgument.capture(), Mockito.any(File.class));
            Assert.assertEquals(S3_BUCKET, bucketArgument.getValue());
            Assert.assertEquals(S3_KEY, keyArgument.getValue());
            Mockito
                .verify(this.uploadTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        }
    }
}
