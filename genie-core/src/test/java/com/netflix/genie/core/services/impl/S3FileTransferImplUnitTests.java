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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
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
     * Test the isValid method for valid file prefix s3n:// or s3://.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testisValidWithValidFilePrefixS3() throws GenieException {
        Assert.assertEquals(s3FileTransfer.isValid("s3://filepath"), true);
    }

    /**
     * Test the isValid method for valid file prefix s3n:// or s3://.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testisValidWithValidFilePrefixS3n() throws GenieException {
        Assert.assertEquals(s3FileTransfer.isValid("s3n://filepath"), true);
    }

    /**
     * Test the isValid method for invalid file prefix not starting with s3n:// or s3://.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testisValidWithInvalidFilePrefix() throws GenieException {
        Assert.assertEquals(s3FileTransfer.isValid("filepath"), false);
    }

    /**
     * Test the getFile method for invalid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testGetFileMethodInvalidS3Path() throws GenieException {
        final String invalidS3Path = "filepath";
        s3FileTransfer.getFile(invalidS3Path, LOCAL_PATH);
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
        s3FileTransfer.getFile(S3_PATH, LOCAL_PATH);
        Mockito
            .verify(this.downloadTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Test the putFile method for invalid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testPutFileMethodInvalidS3Path() throws GenieException {
        final String invalidS3Path = "filepath";
        s3FileTransfer.putFile(LOCAL_PATH, invalidS3Path);
        Mockito.verify(this.uploadTimer, Mockito.times(1)).record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Test the putFile method for valid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
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
        Assert.assertEquals(S3_KEY, bucketArgument.getValue());
        Mockito.verify(this.uploadTimer, Mockito.times(1)).record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
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
        s3FileTransfer.getFile(LOCAL_PATH, S3_PATH);
        Mockito.verify(this.uploadTimer, Mockito.times(1)).record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }
}
