package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Class to test the S3FileTransferImpl class.
 *
 * @author amsharma
 */
@Category(UnitTest.class)
public class S3FileTransferImplUnitTests {

    private S3FileTransferImpl s3FileTransfer;
    /**
     * Setup the tests.
     *
     * @throws GenieException If there is a problem.
     */
    @Before
    public void setup() throws GenieException {
        s3FileTransfer = new S3FileTransferImpl();
    }
    /**
     * Test the getFile method.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    @Ignore
    public void testGetFileMethod() throws GenieException {
        s3FileTransfer.getFile("foo", "bar");
    }
}
