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

import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * A single-use file-backed buffer that stores temporary data.
 * Provides an output stream so data can be written while at the same time being read through the input stream.
 * The temporary file is deleted once close() is invoked.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface FileBuffer extends Closeable {

    /**
     * Get the input stream in order to read the data.
     *
     * @return an input stream
     */
    InputStream getInputStream();

    /**
     * Get the output stream in order to write the data.
     *
     * @return an output stream
     */
    OutputStream getOutputStream();

    /**
     * Get the path of the file backing this buffer.
     *
     * @return a file path
     */
    Path getTemporaryFilePath();
}
