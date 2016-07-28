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

import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.services.FileTransfer;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.Map;

/**
 * This class contains unit tests for GenieFileTransferService.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieFileTransferServiceUnitTests {

    private static final String S3_FILE_PATH = "s3://s3file";
    private static final String LOCAL_FILE_PATH = "file://localfile";

    private LocalFileTransferImpl localFileTransfer;
    private S3FileTransferImpl s3FileTransfer;

    private GenieFileTransferService genieFileTransferService;

    /**
     * Set up the classes for tests.
     *
     * @throws GenieException If there is any problem
     */
    @Before
    public void setUp() throws GenieException {
        localFileTransfer = Mockito.mock(LocalFileTransferImpl.class);
        s3FileTransfer = Mockito.mock(S3FileTransferImpl.class);

        final Map<String, FileTransfer> fileTransferMap = new ImmutableMap.Builder<String, FileTransfer>()
                .put("file.system.s3", s3FileTransfer)
                .put("file.system.file", localFileTransfer).build();

        genieFileTransferService = new GenieFileTransferService(fileTransferMap::get);
    }

    /**
     * Test the getFile method in case none of the File transfer impls can handle the file.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetFileNoValidImplFound() throws GenieException {
        this.genieFileTransferService.getFile("foo", "bar");
    }

    /**
     * Test the getFile method with first implementation in list used.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testGetFileValidImplFoundFirst() throws GenieException {
        Mockito.when(this.localFileTransfer.isValid(Mockito.eq(S3_FILE_PATH))).thenReturn(true);
        Mockito.when(this.s3FileTransfer.isValid(Mockito.eq(S3_FILE_PATH))).thenReturn(false);

        this.genieFileTransferService.getFile(S3_FILE_PATH, LOCAL_FILE_PATH);
        Mockito.verify(this.localFileTransfer, Mockito.times(0)).getFile(S3_FILE_PATH, LOCAL_FILE_PATH);
        Mockito.verify(this.s3FileTransfer, Mockito.times(1)).getFile(S3_FILE_PATH, LOCAL_FILE_PATH);
    }

    /**
     * Test the getFile method with second implementation in list used.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testGetFileValidImplFoundSecond() throws GenieException {
        Mockito.when(this.localFileTransfer.isValid(Mockito.eq(S3_FILE_PATH))).thenReturn(false);
        Mockito.when(this.s3FileTransfer.isValid(Mockito.eq(S3_FILE_PATH))).thenReturn(true);

        this.genieFileTransferService.getFile(S3_FILE_PATH, LOCAL_FILE_PATH);
        Mockito.verify(this.s3FileTransfer, Mockito.times(1)).getFile(S3_FILE_PATH, LOCAL_FILE_PATH);
        Mockito.verify(this.localFileTransfer, Mockito.times(0)).getFile(S3_FILE_PATH, LOCAL_FILE_PATH);
    }

    /**
     * Test the putFile method in case none of the File transfer impls can handle the file.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testPutFileNoValidImplFound() throws GenieException {
        this.genieFileTransferService.putFile("foo", "bar");
    }

    /**
     * Test the getFile method with first implementation in list used.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testPutFileValidImplFoundFirst() throws GenieException {
        Mockito.when(this.localFileTransfer.isValid(Mockito.eq(S3_FILE_PATH))).thenReturn(true);
        Mockito.when(this.s3FileTransfer.isValid(Mockito.eq(S3_FILE_PATH))).thenReturn(false);

        this.genieFileTransferService.putFile(LOCAL_FILE_PATH, S3_FILE_PATH);
        Mockito.verify(this.localFileTransfer, Mockito.times(0)).putFile(LOCAL_FILE_PATH, S3_FILE_PATH);
        Mockito.verify(this.s3FileTransfer, Mockito.times(1)).putFile(LOCAL_FILE_PATH, S3_FILE_PATH);
    }

    /**
     * Test the getFile method with second implementation in list used.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testPutFileValidImplFoundSecond() throws GenieException {
        Mockito.when(this.localFileTransfer.isValid(Mockito.eq(S3_FILE_PATH))).thenReturn(false);
        Mockito.when(this.s3FileTransfer.isValid(Mockito.eq(S3_FILE_PATH))).thenReturn(true);

        this.genieFileTransferService.putFile(LOCAL_FILE_PATH, S3_FILE_PATH);
        Mockito.verify(this.s3FileTransfer, Mockito.times(1)).putFile(LOCAL_FILE_PATH, S3_FILE_PATH);
        Mockito.verify(this.localFileTransfer, Mockito.times(0)).putFile(LOCAL_FILE_PATH, S3_FILE_PATH);
    }
}
