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
package com.netflix.genie.common.internal.services.impl

import com.netflix.genie.common.internal.exceptions.checked.JobArchiveException
import org.apache.commons.codec.digest.DigestUtils
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.charset.StandardCharsets
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitor
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * Specifications for {@link FileSystemJobArchiverImpl}.
 *
 * @author tgianos
 */
class FileSystemJobArchiverImplSpec extends Specification {

    @TempDir
    Path temporaryFolder

    def "Can archive job directory"() {
        def source = Files.createDirectory(this.temporaryFolder.resolve(UUID.randomUUID().toString()))
        def target = Files.createDirectory(this.temporaryFolder.resolve(UUID.randomUUID().toString()))
        def archiver = new FileSystemJobArchiverImpl()

        // create a directory structure
        def genieDir = Files.createDirectory(source.resolve("genie"))
        def genieSubDir = Files.createDirectory(genieDir.resolve("subdir"))
        def clusterDir = Files.createDirectory(genieDir.resolve("cluster"))

        // create files
        def filePaths = [
            source.resolve("stdout"),
            source.resolve("stderr"),
            genieDir.resolve("env.sh"),
            genieSubDir.resolve("exit"),
            clusterDir.resolve("clusterSetupFile.sh"),
        ]

        filePaths.forEach({
            path -> Files.write(path, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
        })

        def sourceVisitor = new FileListVisitor(source)
        Files.walkFileTree(source, sourceVisitor)

        // create one more -- should NOT get archived!
        Files.write(source.resolve("temp.tar.gz"), UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))

        when: "We list the target directory"
        Stream<Path> paths = Files.list(target)

        then: "It is empty"
        paths.count() == 0

        when: "We archive the source to the target and compare the directories"
        def filesList = filePaths.stream().map({ path -> path.toFile() }).collect(Collectors.toList())
        archiver.archiveDirectory(source, filesList, target.toUri())

        then:
        noExceptionThrown()

        when:
        def targetVisitor = new FileListVisitor(target)
        Files.walkFileTree(target, targetVisitor)

        then:
        filePaths.size() == targetVisitor.visitedFiles.size()
        sourceVisitor.visitedFilesAndDirectories == targetVisitor.visitedFilesAndDirectories
        sourceVisitor.visitedFiles == targetVisitor.visitedFiles
    }

    class FileListVisitor implements FileVisitor<Path> {
        Path root
        Map<Path, byte[]> visitedFiles = [:]
        List<Path> visitedFilesAndDirectories = []

        FileListVisitor(Path root) {
            this.root = root
        }

        @Override
        FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            visitedFilesAndDirectories.add(this.root.relativize(dir))
            return FileVisitResult.CONTINUE
        }

        @Override
        FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            def relativePath = this.root.relativize(file)
            visitedFiles.put(relativePath, DigestUtils.md5(Files.readAllBytes(file)))
            visitedFilesAndDirectories.add(relativePath)
            return FileVisitResult.CONTINUE
        }

        @Override
        FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
            return FileVisitResult.CONTINUE
        }

        @Override
        FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
            return FileVisitResult.CONTINUE
        }
    }

    def "Test error cases"() {
        def archiver = new FileSystemJobArchiverImpl()

        when: "source doesn't exist"
        def sourceDoesNotExist = this.temporaryFolder.resolve(UUID.randomUUID().toString())
        archiver.archiveDirectory(
            sourceDoesNotExist,
            [],
            Files.createDirectory(this.temporaryFolder.resolve(UUID.randomUUID().toString())).toUri()
        )

        then: "An exception is thrown"
        thrown(JobArchiveException)

        when: "source isn't a directory"
        def sourceIsFile = Files.createFile(this.temporaryFolder.resolve(UUID.randomUUID().toString()))
        archiver.archiveDirectory(
            sourceIsFile,
            [],
            Files.createDirectory(this.temporaryFolder.resolve(UUID.randomUUID().toString())).toUri()
        )

        then: "An exception is thrown"
        thrown(JobArchiveException)

        when: "Target exists but is a file"
        archiver.archiveDirectory(
            Files.createDirectory(this.temporaryFolder.resolve(UUID.randomUUID().toString())),
            [],
            Files.createFile(this.temporaryFolder.resolve(UUID.randomUUID().toString())).toUri()
        )

        then: "An exception is thrown"
        thrown(JobArchiveException)

        when: "Target cannot be copied"
        def target = Paths.get("/dev/null/foo")
        def file = Files.createFile(this.temporaryFolder.resolve(UUID.randomUUID().toString())).toFile()
        archiver.archiveDirectory(
            this.temporaryFolder,
            [file],
            target.toUri()
        )

        then:
        noExceptionThrown()
    }

    def "Test reject"() {
        def archiver = new FileSystemJobArchiverImpl()

        when:
        def accept = archiver.archiveDirectory(this.temporaryFolder, [], new URI("ftp:///foo/bar"))

        then:
        !accept
    }
}
