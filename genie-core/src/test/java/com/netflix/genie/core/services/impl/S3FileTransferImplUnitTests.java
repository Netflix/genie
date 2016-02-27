package com.netflix.genie.core.services.impl;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;

/**
 * Class to test the S3FileTransferImpl class.
 *
 * @author amsharma
 */
@Category(UnitTest.class)
public class S3FileTransferImplUnitTests {

    private static final String S3_PREFIX = "s3://";
    private static final String S3_BUCKET = "bucket";
    private static final String S3_KEY = "key";
    private static final String S3_PATH = S3_PREFIX
        + S3_BUCKET
        + "/"
        + S3_KEY;
    private static final String LOCAL_PATH = "local";

    private S3FileTransferImpl s3FileTransfer;
    private AmazonS3Client s3Client;

    /**
     * Setup the tests.
     *
     * @throws GenieException If there is a problem.
     */
    @Before
    public void setup() throws GenieException {
        s3Client = Mockito.mock(AmazonS3Client.class);
        s3FileTransfer = new S3FileTransferImpl(s3Client);
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
    }

    /**
     * Test the getFile method for valid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testGetFileMethodFailureToFetch() throws GenieException {

        Mockito.when(this.s3Client.getObject(Mockito.any(GetObjectRequest.class), Mockito.any(File.class)))
            .thenThrow(AmazonS3Exception.class);
        s3FileTransfer.getFile(S3_PATH, LOCAL_PATH);
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
    }

    /**
     * Test the putFile method for valid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testPutFileMethodValidS3Path() throws GenieException {

        final PutObjectResult putObjectResult = Mockito.mock(PutObjectResult.class);
        Mockito.when(this.s3Client.putObject(Mockito.any(), Mockito.any(), Mockito.any()))
            .thenReturn(putObjectResult);
        final ArgumentCaptor<String> bucketArgument = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> keyArgument = ArgumentCaptor.forClass(String.class);

        s3FileTransfer.putFile(LOCAL_PATH, S3_PATH);
        Mockito.verify(this.s3Client).putObject(bucketArgument.capture(), keyArgument.capture(), Mockito.any());
        Assert.assertEquals(S3_BUCKET, bucketArgument.getValue());
        Assert.assertEquals(S3_KEY, bucketArgument.getValue());
    }

    /**
     * Test the putFile method for valid s3 path.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testPutFileMethodFailureToFetch() throws GenieException {

        Mockito.when(this.s3Client.putObject(Mockito.any(), Mockito.any(), Mockito.any()))
            .thenThrow(AmazonS3Exception.class);
        s3FileTransfer.getFile(LOCAL_PATH, S3_PATH);
    }
}
