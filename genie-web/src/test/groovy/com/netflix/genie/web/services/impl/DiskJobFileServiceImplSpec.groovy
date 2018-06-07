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
package com.netflix.genie.web.services.impl

import com.google.common.collect.Sets
import com.netflix.genie.common.internal.dto.v4.files.JobFileState
import com.netflix.genie.test.categories.UnitTest
import org.apache.commons.codec.digest.DigestUtils
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import org.springframework.core.io.PathResource
import spock.lang.Specification

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
/**
 * Specifications for the {@link DiskJobFileServiceImpl} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class DiskJobFileServiceImplSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder

    DiskJobFileServiceImpl diskLogService

    def setup() {
        def jobDirRoot = new PathResource(this.temporaryFolder.getRoot().toPath())
        this.diskLogService = new DiskJobFileServiceImpl(jobDirRoot)
    }

    def "Can create job log directory"() {
        def jobId = UUID.randomUUID().toString()

        when:
        this.diskLogService.createJobDirectory(jobId)
        def jobDir = this.temporaryFolder.getRoot().toPath().resolve(jobId)

        then:
        Files.exists(jobDir)
        Files.isDirectory(jobDir)
        Files.isWritable(jobDir)
    }

    def "Can update and delete log files and get job directory state"() {
        def utf8 = Charset.forName("UTF-8")
        def separator = File.separator
        def jobId = UUID.randomUUID().toString()
        def file1 = UUID.randomUUID().toString() + ".txt"
        def file2 = "GenieLogo.png"
        def file3 =
                UUID.randomUUID().toString() +
                        separator +
                        UUID.randomUUID().toString() +
                        separator +
                        UUID.randomUUID().toString() +
                        ".log"
        def jobDir = this.temporaryFolder.getRoot().toPath().resolve(jobId)
        def file1Path = jobDir.resolve(file1)
        def file2Path = jobDir.resolve(file2)
        def file3Path = jobDir.resolve(file3)

        def file1Contents = "Log 1\nDid some stuff\ndoing some more stuff\nsheesh still not done"
        def file1ContentsAsBytes = file1Contents.getBytes(utf8)
        def file1StartByte = 0L
        def file1Length = (long) (file1StartByte + file1ContentsAsBytes.length)

        def file2SourcePath = Paths.get(this.getClass().getResource(file2).toURI())
        def file2ContentsAsBytes = Files.readAllBytes(file2SourcePath)
        def file2StartByte = 0L
        def file2Length = (long) (file2StartByte + file2ContentsAsBytes.length)

        def file3Contents = UUID.randomUUID().toString() + UUID.randomUUID().toString()
        def file3ContentsAsBytes = file3Contents.getBytes(utf8)
        def file3StartByte = 10_003L
        def file3Length = (long) (file3StartByte + file3ContentsAsBytes.length)


        when:
        this.diskLogService.createJobDirectory(jobId)

        then:
        Files.exists(jobDir)
        Files.notExists(file1Path)
        Files.notExists(file2Path)
        Files.notExists(file3Path)

        when: "Write some starting bytes in a file"
        this.diskLogService.updateFile(jobId, file1, file1StartByte, file1ContentsAsBytes)

        then:
        Files.exists(jobDir)
        Files.exists(file1Path)
        Files.notExists(file2Path)
        Files.notExists(file3Path)
        Files.size(file1Path) == file1Length
        Files.lines(file1Path).reduce { line1, line2 -> line1 + "\n" + line2 }.orElse(null) == file1Contents

        when: "Write a binary file"
        this.diskLogService.updateFile(jobId, file2, file2StartByte, file2ContentsAsBytes)

        then:
        Files.exists(jobDir)
        Files.exists(file1Path)
        Files.exists(file2Path)
        Files.notExists(file3Path)
        Files.size(file2Path) == file2Length
        Files.readAllBytes(file2Path) == Files.readAllBytes(file2SourcePath)

        when: "Write random bytes into a file in nested directories"
        this.diskLogService.updateFile(jobId, file3, file3StartByte, file3ContentsAsBytes)
        def file3RealContents = Files.readAllBytes(file3Path)

        then: "All parent directories are created and the file is total size"
        Files.exists(jobDir)
        Files.exists(file1Path)
        Files.exists(file2Path)
        Files.exists(file3Path.getParent().getParent())
        Files.exists(file3Path.getParent())
        Files.exists(file3Path)
        Files.size(file3Path) == file3Length
        // This shouldn't be equal since we effectively padded the file with a bunch of junk
        file3RealContents != file3ContentsAsBytes
        Arrays.copyOfRange(file3RealContents, (int) file3StartByte, file3RealContents.length) == file3ContentsAsBytes

        when: "Try to overwrite previous part of existing log file"
        def additionalFile3Contents = UUID.randomUUID().toString().getBytes(utf8)
        this.diskLogService.updateFile(jobId, file3, 0L, additionalFile3Contents)
        file3RealContents = Files.readAllBytes(file3Path)

        then: "All other contents remained the same but the first section of the file changed"
        Files.exists(file3Path)
        // The size didn't change
        Files.size(file3Path) == file3Length
        // This remains the same
        Arrays.copyOfRange(file3RealContents, (int) file3StartByte, file3RealContents.length) == file3ContentsAsBytes
        Arrays.copyOfRange(file3RealContents, 0, additionalFile3Contents.length) == additionalFile3Contents

        when: "Directory state is read without MD5 it matches expected output"
        def expectedJobDirectoryStateWithoutMd5 = Sets.newHashSet(
                new JobFileState(file1, file1Length, null),
                new JobFileState(file2, file2Length, null),
                new JobFileState(file3, file3Length, null)
        )
        def jobDirectoryState = this.diskLogService.getJobDirectoryFileState(jobId, false)

        then:
        jobDirectoryState == expectedJobDirectoryStateWithoutMd5

        when: "Directory state is read with MD5 it matches expected output"
        def expectedJobDirectoryStateWithMd5 = Sets.newHashSet(
                new JobFileState(file1, file1Length, DigestUtils.md5Hex(file1ContentsAsBytes)),
                new JobFileState(file2, file2Length, DigestUtils.md5Hex(file2ContentsAsBytes)),
                new JobFileState(file3, file3Length, DigestUtils.md5Hex(file3RealContents))
        )
        jobDirectoryState = this.diskLogService.getJobDirectoryFileState(jobId, true)

        then:
        jobDirectoryState != expectedJobDirectoryStateWithoutMd5
        jobDirectoryState == expectedJobDirectoryStateWithMd5

        when: "Directory is modified directory state is no longer consistent"
        this.diskLogService.updateFile(jobId, file3, 100L, UUID.randomUUID().toString().getBytes(utf8))
        jobDirectoryState = this.diskLogService.getJobDirectoryFileState(jobId, true)
        def jobDirectoryStateWithoutMd5 = this.diskLogService.getJobDirectoryFileState(jobId, false)

        then:
        "Since we wrote bytes in the middle of a file the md5 comparison will fail but the naive one based on " +
                "size won't"
        jobDirectoryState != expectedJobDirectoryStateWithMd5
        jobDirectoryStateWithoutMd5 == expectedJobDirectoryStateWithoutMd5

        when: "A file that exists is requested to be deleted"
        this.diskLogService.deleteJobFile(jobId, file2)

        then: "It is deleted"
        Files.exists(file1Path)
        Files.notExists(file2Path)
        Files.exists(file3Path)

        when: "A file that doesn't exist is requested to be deleted"
        this.diskLogService.deleteJobFile(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        this.diskLogService.deleteJobFile(jobId, file3)

        then: "Nothing happens"
        Files.exists(file1Path)
        Files.notExists(file2Path)
        Files.notExists(file3Path)
    }

    def "Can get log file resource"() {
        def utf8 = Charset.forName("UTF-8")
        def jobId = UUID.randomUUID().toString()
        def file = UUID.randomUUID().toString() + ".txt"

        when:
        def resource = this.diskLogService.getJobFileAsResource(jobId, file)

        then:
        resource instanceof PathResource
        !resource.exists()

        when:
        this.diskLogService.updateFile(jobId, file, 0L, UUID.randomUUID().toString().getBytes(utf8))

        then:
        resource.exists()

        when:
        resource = this.diskLogService.getJobFileAsResource(jobId, null)

        then:
        resource.exists()
        resource.getFile().isDirectory()
        resource.getFile().toPath() == this.temporaryFolder.getRoot().toPath().resolve(jobId)
    }
}
