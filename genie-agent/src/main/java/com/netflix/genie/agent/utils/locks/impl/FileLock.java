/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.agent.utils.locks.impl;

import com.netflix.genie.agent.execution.exceptions.LockException;
import com.netflix.genie.agent.utils.locks.CloseableLock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * CloseableLock for a file.
 *
 * @author standon
 * @since 4.0.0
 */
public class FileLock implements CloseableLock {

    //Refer to https://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html
    private static final String FILE_ACCESS_MODE = "rws";
    private FileChannel fileChannel;
    //Maintain a link to the underlying nio file lock because of
    //https://bugs.openjdk.java.net/browse/JDK-8166253
    private java.nio.channels.FileLock nioFileLock;

    /**
     * Create a lock for the provided file.
     * @param file file to be locked
     * @throws LockException in case there is a problem creating a File CloseableLock
     */
    public FileLock(final File file) throws LockException {

        if (file == null || !file.exists()) {
            throw new LockException("File is null or does not exist");
        }

        try {
            fileChannel = new RandomAccessFile(
                file,
                FILE_ACCESS_MODE
            ).getChannel();
        } catch (Exception e) {
            throw new LockException("Error creating a FileLock ", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        //FileChannel.close closes the nioFileLock. Closing
        //it explicitly, else findbugs rule URF_UNREAD_FIELD is violated
        nioFileLock.close();
        fileChannel.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void lock() throws LockException {
        try {
            nioFileLock = fileChannel.lock();
        } catch (Exception e) {
            throw new LockException("Error locking file ", e);
        }
    }
}
