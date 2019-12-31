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
package com.netflix.genie.web.resources.writers;

import com.netflix.genie.common.external.util.GenieObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.w3c.tidy.Tidy;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * Unit tests for the DefaultDirectoryWriter.
 *
 * @author tgianos
 * @since 3.0.0
 */
class DefaultDirectoryWriterTest {

    private static final String REQUEST_URL_BASE
        = "http://genie.netflix.com:8080/api/v3/jobs/" + UUID.randomUUID().toString() + "/output";
    private static final String REQUEST_URL_WITH_PARENT = REQUEST_URL_BASE + "/" + UUID.randomUUID().toString();

    private static final long PARENT_SIZE = 0L;
    private static final Instant PARENT_LAST_MODIFIED = Instant.now();
    private static final String PARENT_NAME = "../";
    private static final String PARENT_URL = REQUEST_URL_BASE;

    private static final long DIR_1_SIZE = 0L;
    private static final Instant DIR_1_LAST_MODIFIED = Instant.now().plus(13142, ChronoUnit.MILLIS);
    private static final String DIR_1_NAME = UUID.randomUUID().toString();
    private static final String DIR_1_URL = REQUEST_URL_WITH_PARENT + "/" + DIR_1_NAME;

    private static final long DIR_2_SIZE = 0L;
    private static final Instant DIR_2_LAST_MODIFIED = Instant.now().minus(1830, ChronoUnit.MILLIS);
    private static final String DIR_2_NAME = UUID.randomUUID().toString();
    private static final String DIR_2_URL = REQUEST_URL_WITH_PARENT + "/" + DIR_2_NAME;

    private static final long FILE_1_SIZE = 73522431;
    private static final Instant FILE_1_LAST_MODIFIED = Instant.now().plus(1832430, ChronoUnit.MILLIS);
    private static final String FILE_1_NAME = UUID.randomUUID().toString();
    private static final String FILE_1_URL = REQUEST_URL_WITH_PARENT + "/" + FILE_1_NAME;

    private static final long FILE_2_SIZE = 735231;
    private static final Instant FILE_2_LAST_MODIFIED = Instant.now().plus(1832443, ChronoUnit.MILLIS);
    private static final String FILE_2_NAME = UUID.randomUUID().toString();
    private static final String FILE_2_URL = REQUEST_URL_WITH_PARENT + "/" + FILE_2_NAME;

    private DefaultDirectoryWriter writer;
    private File directory;
    private DefaultDirectoryWriter.Entry directoryEntry1;
    private DefaultDirectoryWriter.Entry directoryEntry2;
    private DefaultDirectoryWriter.Entry fileEntry1;
    private DefaultDirectoryWriter.Entry fileEntry2;

    /**
     * Setup the tests.
     */
    @BeforeEach
    void setup() {
        this.writer = new DefaultDirectoryWriter();
        this.directory = Mockito.mock(File.class);
        Mockito.when(this.directory.isDirectory()).thenReturn(true);

        final String slash = "/";
        this.directoryEntry1 = new DefaultDirectoryWriter.Entry();
        this.directoryEntry1.setLastModified(DIR_1_LAST_MODIFIED);
        this.directoryEntry1.setSize(DIR_1_SIZE);
        this.directoryEntry1.setUrl(DIR_1_URL + slash);
        this.directoryEntry1.setName(DIR_1_NAME + slash);

        this.directoryEntry2 = new DefaultDirectoryWriter.Entry();
        this.directoryEntry2.setLastModified(DIR_2_LAST_MODIFIED);
        this.directoryEntry2.setSize(DIR_2_SIZE);
        this.directoryEntry2.setUrl(DIR_2_URL + slash);
        this.directoryEntry2.setName(DIR_2_NAME + slash);

        this.fileEntry1 = new DefaultDirectoryWriter.Entry();
        this.fileEntry1.setLastModified(FILE_1_LAST_MODIFIED);
        this.fileEntry1.setSize(FILE_1_SIZE);
        this.fileEntry1.setUrl(FILE_1_URL);
        this.fileEntry1.setName(FILE_1_NAME);

        this.fileEntry2 = new DefaultDirectoryWriter.Entry();
        this.fileEntry2.setLastModified(FILE_2_LAST_MODIFIED);
        this.fileEntry2.setSize(FILE_2_SIZE);
        this.fileEntry2.setUrl(FILE_2_URL);
        this.fileEntry2.setName(FILE_2_NAME);
    }

    /**
     * Make sure if the argument passed in isn't a directory an exception is thrown.
     */
    @Test
    void cantGetDirectoryWithoutValidDirectory() {
        Mockito.when(this.directory.isDirectory()).thenReturn(false);
        Assertions
            .assertThatIllegalArgumentException()
            .isThrownBy(() -> this.writer.getDirectory(this.directory, UUID.randomUUID().toString(), false));
    }

    /**
     * Make sure an exception is thrown when no request URL is passed in to the method.
     */
    @Test
    void cantGetDirectoryWithoutRequestUrl() {
        Assertions
            .assertThatIllegalArgumentException()
            .isThrownBy(() -> this.writer.getDirectory(this.directory, null, false));
    }

    /**
     * Make sure can get a directory with a parent.
     */
    @Test
    void canGetDirectoryWithParent() {
        this.setupWithParent();
        final DefaultDirectoryWriter.Directory dir
            = this.writer.getDirectory(this.directory, REQUEST_URL_WITH_PARENT, true);

        Assertions.assertThat(dir.getParent()).isNotNull();
        Assertions.assertThat(dir.getParent().getName()).isEqualTo(PARENT_NAME);
        Assertions.assertThat(dir.getParent().getUrl()).isEqualTo(PARENT_URL);
        Assertions.assertThat(dir.getParent().getSize()).isEqualTo(PARENT_SIZE);
        Assertions.assertThat(dir.getParent().getLastModified()).isEqualTo(PARENT_LAST_MODIFIED);

        Assertions.assertThat(dir.getDirectories()).isNotNull();
        Assertions
            .assertThat(dir.getDirectories())
            .containsExactlyInAnyOrder(this.directoryEntry1, this.directoryEntry2);

        Assertions.assertThat(dir.getFiles()).isNotNull();
        Assertions.assertThat(dir.getFiles()).containsExactlyInAnyOrder(this.fileEntry1, this.fileEntry2);
    }

    /**
     * Make sure can get a directory without a parent.
     */
    @Test
    void canGetDirectoryWithoutParent() {
        this.setupWithoutParent();
        final DefaultDirectoryWriter.Directory dir
            = this.writer.getDirectory(this.directory, REQUEST_URL_BASE, false);

        Assertions.assertThat(dir.getParent()).isNull();

        Assertions.assertThat(dir.getDirectories()).isNotNull();
        Assertions.assertThat(dir.getDirectories().isEmpty()).isTrue();

        Assertions.assertThat(dir.getFiles()).isNotNull();
        Assertions.assertThat(dir.getFiles().isEmpty()).isTrue();
    }

    /**
     * Make sure can get html representation of the directory.
     *
     * @throws Exception on any problem
     */
    @Test
    void canConvertToHtml() throws Exception {
        this.setupWithParent();
        final String html = this.writer.toHtml(this.directory, REQUEST_URL_WITH_PARENT, true);
        Assertions.assertThat(html).isNotNull();

        // Not going to parse the whole HTML to validate contents, too much work.
        // So just make sure HTML is valid so at least it doesn't cause error in browser
        final Tidy tidy = new Tidy();
        final Writer stringWriter = new StringWriter();
        tidy.parse(new ByteArrayInputStream(html.getBytes(StandardCharsets.UTF_8)), stringWriter);
        Assertions.assertThat(tidy.getParseErrors()).isEqualTo(0);
        Assertions.assertThat(tidy.getParseWarnings()).isEqualTo(0);
    }

    /**
     * Make sure can get a json representation of the directory.
     *
     * @throws Exception on any problem
     */
    @Test
    void canConvertToJson() throws Exception {
        this.setupWithParent();
        final String json = this.writer.toJson(this.directory, REQUEST_URL_WITH_PARENT, true);
        Assertions.assertThat(json).isNotNull();
        final DefaultDirectoryWriter.Directory dir
            = GenieObjectMapper.getMapper().readValue(json, DefaultDirectoryWriter.Directory.class);

        Assertions.assertThat(dir.getParent()).isNotNull();
        Assertions.assertThat(dir.getParent().getName()).isEqualTo(PARENT_NAME);
        Assertions.assertThat(dir.getParent().getUrl()).isEqualTo(PARENT_URL);
        Assertions.assertThat(dir.getParent().getSize()).isEqualTo(PARENT_SIZE);
        Assertions.assertThat(dir.getParent().getLastModified()).isEqualTo(PARENT_LAST_MODIFIED);

        Assertions.assertThat(dir.getDirectories()).isNotNull();
        Assertions
            .assertThat(dir.getDirectories())
            .containsExactlyInAnyOrder(this.directoryEntry1, this.directoryEntry2);

        Assertions.assertThat(dir.getFiles()).isNotNull();
        Assertions
            .assertThat(dir.getFiles())
            .containsExactlyInAnyOrder(this.fileEntry1, this.fileEntry2);
    }

    private void setupWithoutParent() {
        Mockito.when(this.directory.listFiles()).thenReturn(null);
    }

    private void setupWithParent() {
        final File parent = Mockito.mock(File.class);
        final File absoluteParent = Mockito.mock(File.class);
        Mockito.when(parent.getAbsoluteFile()).thenReturn(absoluteParent);
        Mockito.when(absoluteParent.length()).thenReturn(PARENT_SIZE);
        Mockito.when(absoluteParent.lastModified()).thenReturn(PARENT_LAST_MODIFIED.toEpochMilli());
        Mockito.when(parent.getName()).thenReturn(PARENT_NAME);

        Mockito.when(this.directory.getParentFile()).thenReturn(parent);

        final File dir1 = Mockito.mock(File.class);
        final File absoluteDir1 = Mockito.mock(File.class);
        Mockito.when(dir1.getAbsoluteFile()).thenReturn(absoluteDir1);
        Mockito.when(absoluteDir1.length()).thenReturn(DIR_1_SIZE);
        Mockito.when(absoluteDir1.lastModified()).thenReturn(DIR_1_LAST_MODIFIED.toEpochMilli());
        Mockito.when(dir1.getName()).thenReturn(DIR_1_NAME);
        Mockito.when(dir1.isDirectory()).thenReturn(true);

        final File dir2 = Mockito.mock(File.class);
        final File absoluteDir2 = Mockito.mock(File.class);
        Mockito.when(dir2.getAbsoluteFile()).thenReturn(absoluteDir2);
        Mockito.when(absoluteDir2.length()).thenReturn(DIR_2_SIZE);
        Mockito.when(absoluteDir2.lastModified()).thenReturn(DIR_2_LAST_MODIFIED.toEpochMilli());
        Mockito.when(dir2.getName()).thenReturn(DIR_2_NAME);
        Mockito.when(dir2.isDirectory()).thenReturn(true);

        final File file1 = Mockito.mock(File.class);
        final File absoluteFile1 = Mockito.mock(File.class);
        Mockito.when(file1.getAbsoluteFile()).thenReturn(absoluteFile1);
        Mockito.when(absoluteFile1.length()).thenReturn(FILE_1_SIZE);
        Mockito.when(absoluteFile1.lastModified()).thenReturn(FILE_1_LAST_MODIFIED.toEpochMilli());
        Mockito.when(file1.getName()).thenReturn(FILE_1_NAME);
        Mockito.when(file1.isDirectory()).thenReturn(false);

        final File file2 = Mockito.mock(File.class);
        final File absoluteFile2 = Mockito.mock(File.class);
        Mockito.when(file2.getAbsoluteFile()).thenReturn(absoluteFile2);
        Mockito.when(absoluteFile2.length()).thenReturn(FILE_2_SIZE);
        Mockito.when(absoluteFile2.lastModified()).thenReturn(FILE_2_LAST_MODIFIED.toEpochMilli());
        Mockito.when(file2.getName()).thenReturn(FILE_2_NAME);
        Mockito.when(file2.isDirectory()).thenReturn(false);

        final File[] files = new File[]{dir1, file1, dir2, file2};
        Mockito.when(this.directory.listFiles()).thenReturn(files);
    }
}
