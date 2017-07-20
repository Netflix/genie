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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * This class contains unit tests for the class LocalFileTransferImpl.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class LocalFileTransferImplUnitTests {

    /**
     * Temporary folder for test files.
     */
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private LocalFileTransferImpl localFileTransfer;

    /**
     * Setup the tests.
     *
     * @throws GenieException If there is a problem.
     */
    @Before
    public void setup() throws GenieException {
        this.localFileTransfer = new LocalFileTransferImpl();
    }

    /**
     * Test the isValid method for valid file prefix file://.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void isValidWithCorrectFilePrefix() throws GenieException {
        Assert.assertEquals(this.localFileTransfer.isValid("file:///filepath"), true);
    }

    /**
     * Test the getFile method.
     *
     * @throws GenieException If there is any problem
     * @throws IOException    If there is any problem
     */
    @Test
    public void canGetFile() throws GenieException, IOException {
        final String srcFile = this.temporaryFolder.newFile().getAbsolutePath();
        final String fileName = UUID.randomUUID().toString();
        final File dstFile = new File(this.temporaryFolder.getRoot().getAbsolutePath(), fileName);

        Assert.assertFalse(dstFile.exists());
        this.localFileTransfer.getFile(srcFile, dstFile.getAbsolutePath());
        Assert.assertTrue(dstFile.exists());

        // If a directory exists it can copy the file
        final File folder = this.temporaryFolder.newFolder();
        final File dstFile2 = new File(folder.getAbsolutePath(), fileName);
        Assert.assertTrue(folder.exists());
        Assert.assertFalse(dstFile2.exists());
        this.localFileTransfer.getFile(srcFile, dstFile2.getAbsolutePath());
        Assert.assertTrue(dstFile2.exists());

        // If a directory doesn't exist it creates it and copies the file
        final File notExistsFolder = new File(this.temporaryFolder.getRoot(), UUID.randomUUID().toString());
        Assert.assertFalse(notExistsFolder.exists());
        final File dstFile3 = new File(notExistsFolder, UUID.randomUUID().toString());
        Assert.assertFalse(dstFile3.exists());
        this.localFileTransfer.getFile(srcFile, dstFile3.getAbsolutePath());
        Assert.assertTrue(dstFile3.exists());

        // Normalize for case where input is file:
        final File dstFile4 = new File(folder.getAbsolutePath(), UUID.randomUUID().toString());
        Assert.assertFalse(dstFile4.exists());
        this.localFileTransfer.getFile("file:" + srcFile, dstFile4.getAbsolutePath());
        Assert.assertTrue(dstFile4.exists());

        // Normalize for case where input is file://
        final File dstFile5 = new File(folder.getAbsolutePath(), UUID.randomUUID().toString());
        Assert.assertFalse(dstFile5.exists());
        this.localFileTransfer.getFile("file://" + srcFile, dstFile5.getAbsolutePath());
        Assert.assertTrue(dstFile5.exists());
    }

    /**
     * Test the putFile method.
     *
     * @throws GenieException If there is any problem
     * @throws IOException    If there is any problem
     */
    @Test
    public void testPutFile() throws GenieException, IOException {
        final String srcFile = this.temporaryFolder.newFile().getAbsolutePath();
        final String fileName = UUID.randomUUID().toString();
        final File dstFile = new File(this.temporaryFolder.getRoot().getAbsolutePath(), fileName);

        Assert.assertFalse(dstFile.exists());
        this.localFileTransfer.putFile(srcFile, dstFile.getAbsolutePath());
        Assert.assertTrue(dstFile.exists());

        // If a directory exists it can copy the file
        final File folder = this.temporaryFolder.newFolder();
        final File dstFile2 = new File(folder.getAbsolutePath(), fileName);
        Assert.assertTrue(folder.exists());
        Assert.assertFalse(dstFile2.exists());
        this.localFileTransfer.putFile(srcFile, dstFile2.getAbsolutePath());
        Assert.assertTrue(dstFile2.exists());

        // If a directory doesn't exist it creates it and copies the file
        final File notExistsFolder = new File(this.temporaryFolder.getRoot(), UUID.randomUUID().toString());
        Assert.assertFalse(notExistsFolder.exists());
        final File dstFile3 = new File(notExistsFolder, UUID.randomUUID().toString());
        Assert.assertFalse(dstFile3.exists());
        this.localFileTransfer.putFile(srcFile, dstFile3.getAbsolutePath());
        Assert.assertTrue(dstFile3.exists());
    }

    /**
     * Make sure the last modified time is accurate.
     *
     * @throws GenieException on error
     * @throws IOException    on error
     */
    @Test
    public void canGetLastModifiedTime() throws GenieException, IOException {
        final File file = this.temporaryFolder.newFile();
        final long lastModifiedTime = file.lastModified();
        Assert.assertThat(
            this.localFileTransfer.getLastModifiedTime(file.getAbsolutePath()),
            Matchers.is(lastModifiedTime)
        );
    }
}
