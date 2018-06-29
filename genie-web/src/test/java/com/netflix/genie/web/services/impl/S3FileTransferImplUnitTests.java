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
package com.netflix.genie.web.services.impl;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.properties.S3FileTransferProperties;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.util.Map;
import java.util.Set;
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
    private static final Set<Tag> SUCCESS_TAGS = MetricsUtils.newSuccessTagsSet();
    private static final Set<Tag> FAILURE_TAGS
        = MetricsUtils.newFailureTagsSetForException(new GenieBadRequestException("test"));

    private MeterRegistry registry;
    private S3FileTransferImpl s3FileTransfer;
    private AmazonS3Client s3Client;
    private S3FileTransferProperties s3FileTransferProperties;
    private Timer downloadTimer;
    private Timer uploadTimer;
    private Counter urlFailingStrictValidationCounter;
    @Captor
    private ArgumentCaptor<Set<Tag>> tagsCaptor;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.registry = Mockito.mock(MeterRegistry.class);
        this.downloadTimer = Mockito.mock(Timer.class);
        this.uploadTimer = Mockito.mock(Timer.class);
        this.urlFailingStrictValidationCounter = Mockito.mock(Counter.class);
        this.s3Client = Mockito.mock(AmazonS3Client.class);
        Mockito.
            when(registry.timer(Mockito.eq(S3FileTransferImpl.DOWNLOAD_TIMER_NAME), Mockito.anySet()))
            .thenReturn(this.downloadTimer);
        Mockito.
            when(registry.timer(Mockito.eq(S3FileTransferImpl.UPLOAD_TIMER_NAME), Mockito.anySet()))
            .thenReturn(this.uploadTimer);
        Mockito
            .when(registry.counter(S3FileTransferImpl.STRICT_VALIDATION_COUNTER_NAME))
            .thenReturn(this.urlFailingStrictValidationCounter);
        this.s3FileTransferProperties = Mockito.mock(S3FileTransferProperties.class);
        this.s3FileTransfer = new S3FileTransferImpl(this.s3Client, this.registry, this.s3FileTransferProperties);
    }

    /**
     * Given a set of valid S3 {prefix,bucket,key}, try all combinations.
     * Ensure they are accepted as valid and the path components are parsed correctly.
     * Each component is tagged as being valid for strict validation.
     *
     * @throws GenieException in case of error building the URI
     */
    @Test
    public void testValidS3Paths() throws GenieException {
        final Map<String, Boolean> prefixes = ImmutableMap.<String, Boolean>builder()
            .put("s3://", true)
            .put("s3n://", true)
            .build();

        final Map<String, Boolean> buckets = ImmutableMap.<String, Boolean>builder()
            .put(".", false)
            .put("b", false)
            .put("bucket", true)
            .put("Bucket", false)
            .put("bucket1", true)
            .put("1bucket", true)
            .put("bucket-bucket", true)
            .put("bucket.bucket", true)
            .put("bucket+bucket", false)
            .put(".bucket", false)
            .put("bucket.", false)
            .put("buc:ket", false)
            .put("buc!ket", false)
            .put("buc(ket", false)
            .put("buc'ket", false)
            .put(StringUtils.leftPad("", 64, "b"), false)
            .build();

        final Map<String, Boolean> keys = ImmutableMap.<String, Boolean>builder()
            .put("Development/Projects1.xls", true)
            .put("Finance/statement1.pdf", true)
            .put("Private/taxdocument.pdf", true)
            .put("s3-dg.pdf", true)
            .put("weird/but/valid/key!-_*'().pdf", true)
            .put("1+1=3.pdf", false)
            .put("/", false)
            .put("//", false)
            .build();

        int notStrictlyValidExpectedCount = 0;

        // Re-run all combinations with stricter check
        for (final Map.Entry<String, Boolean> prefixEntry : prefixes.entrySet()) {
            for (final Map.Entry<String, Boolean> bucketEntry : buckets.entrySet()) {
                for (final Map.Entry<String, Boolean> keyEntry : keys.entrySet()) {

                    final String path = prefixEntry.getKey() + bucketEntry.getKey() + "/" + keyEntry.getKey();
                    final boolean expectedStrictlyValid =
                        prefixEntry.getValue() && bucketEntry.getValue() && keyEntry.getValue();

                    // Turn off strict validation
                    Mockito.when(s3FileTransferProperties.isStrictUrlCheckEnabled()).thenReturn(false);
                    Assert.assertFalse(s3FileTransferProperties.isStrictUrlCheckEnabled());
                    // Should pass non-strict validation
                    Assert.assertTrue(
                        "Failed validation: " + path,
                        this.s3FileTransfer.isValid(path)
                    );
                    // Should correctly split bucket from key
                    final AmazonS3URI s3Uri = this.s3FileTransfer.getS3Uri(path);
                    Assert.assertEquals(bucketEntry.getKey(), s3Uri.getBucket());
                    Assert.assertEquals(keyEntry.getKey(), s3Uri.getKey());

                    if (!expectedStrictlyValid) {
                        // Count twice, for isValid() and getS3Uri()
                        notStrictlyValidExpectedCount += 2;
                    }

                    //Turn on strict validation
                    Mockito.when(s3FileTransferProperties.isStrictUrlCheckEnabled()).thenReturn(true);
                    Assert.assertTrue(s3FileTransferProperties.isStrictUrlCheckEnabled());

                    Assert.assertEquals(
                        "Failed strict validation: " + path,
                        expectedStrictlyValid,
                        this.s3FileTransfer.isValid(path)
                    );
                }
            }
        }

        Mockito
            .verify(urlFailingStrictValidationCounter, Mockito.times(notStrictlyValidExpectedCount))
            .increment();
    }

    /**
     * Verify invalid S3 prefixes are rejected.
     *
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
            Assert.assertFalse(
                "Passed validation: " + path,
                this.s3FileTransfer.isValid(path)
            );
            boolean genieException = false;
            try {
                this.s3FileTransfer.getS3Uri(path);
            } catch (GenieBadRequestException e) {
                genieException = true;
            } finally {
                Assert.assertTrue(
                    "Parsed without error: " + path,
                    genieException
                );
            }
        }
    }

    /**
     * Verify invalid S3 bucket names are rejected.
     *
     * @throws GenieException in case of unexpected validation error
     */
    @Test
    public void testInvalidS3Buckets() throws GenieException {
        final String[] invalidBuckets = {
            "",
            " ",
            "buc ket",
            "buc[ket",
            "/",
            // "buc..ket", // Invalid, but current logic does not catch this
        };

        for (final String invalidBucket : invalidBuckets) {
            final String path = "s3://" + invalidBucket + "/key";
            Assert.assertFalse(
                "Passed validation: " + path,
                this.s3FileTransfer.isValid(path)
            );
            boolean genieException = false;
            try {
                this.s3FileTransfer.getS3Uri(path);
            } catch (final GenieBadRequestException e) {
                genieException = true;
            } finally {
                Assert.assertTrue(
                    "Parsed without error: " + path,
                    genieException
                );
            }
        }
    }

    /**
     * Verify invalid S3 key names are rejected.
     *
     * @throws GenieException in case of unexpected validation error
     */
    @Test
    public void testInvalidS3Keys() throws GenieException {
        final String[] invalidKeys = {
            "",
            " ",
            "k[ey",
        };

        for (final String invalidKey : invalidKeys) {
            final String path = "s3://bucket/" + invalidKey;
            Assert.assertFalse(
                "Passed validation: " + path,
                this.s3FileTransfer.isValid(path)
            );
            boolean genieException = false;
            try {
                this.s3FileTransfer.getS3Uri(path);
            } catch (GenieBadRequestException e) {
                genieException = true;
            } finally {
                Assert.assertTrue(
                    "Parsed without error: " + path,
                    genieException
                );
            }
        }
    }

    /**
     * Test the getFile method for invalid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieBadRequestException.class)
    public void testGetFileMethodInvalidS3Path() throws GenieException {
        final String invalidS3Path = "filepath";
        try {
            this.s3FileTransfer.getFile(invalidS3Path, LOCAL_PATH);
        } finally {
            Mockito
                .verify(this.downloadTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(Mockito.eq(S3FileTransferImpl.DOWNLOAD_TIMER_NAME), this.tagsCaptor.capture());
            Assert.assertEquals(FAILURE_TAGS, this.tagsCaptor.getValue());
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
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(Mockito.eq(S3FileTransferImpl.DOWNLOAD_TIMER_NAME), this.tagsCaptor.capture());
        Assert.assertEquals(SUCCESS_TAGS, this.tagsCaptor.getValue());

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
            this.s3FileTransfer.getFile(S3_PATH, LOCAL_PATH);
        } finally {
            Mockito.verify(this.s3Client).getObject(argument.capture(), Mockito.any());
            Assert.assertEquals(S3_BUCKET, argument.getValue().getBucketName());
            Assert.assertEquals(S3_KEY, argument.getValue().getKey());
            Mockito
                .verify(this.downloadTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(Mockito.eq(S3FileTransferImpl.DOWNLOAD_TIMER_NAME), this.tagsCaptor.capture());
            Assert.assertEquals(
                MetricsUtils.newFailureTagsSetForException(new GenieServerException("blah")),
                this.tagsCaptor.getValue()
            );
        }
    }

    /**
     * Test the putFile method for invalid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieBadRequestException.class)
    public void testPutFileMethodInvalidS3Path() throws GenieException {
        final String invalidS3Path = "filepath";
        try {
            s3FileTransfer.putFile(LOCAL_PATH, invalidS3Path);
        } finally {
            Mockito
                .verify(this.uploadTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(Mockito.eq(S3FileTransferImpl.UPLOAD_TIMER_NAME), this.tagsCaptor.capture());
            Assert.assertEquals(FAILURE_TAGS, this.tagsCaptor.getValue());
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
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(Mockito.eq(S3FileTransferImpl.UPLOAD_TIMER_NAME), this.tagsCaptor.capture());
        Assert.assertEquals(SUCCESS_TAGS, this.tagsCaptor.getValue());
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
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(Mockito.eq(S3FileTransferImpl.UPLOAD_TIMER_NAME), this.tagsCaptor.capture());
            Assert.assertEquals(
                MetricsUtils.newFailureTagsSetForException(new GenieServerException("blah")),
                this.tagsCaptor.getValue()
            );
        }
    }
}
