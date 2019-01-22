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

import org.apache.commons.codec.digest.DigestUtils
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.util.stream.Stream

/**
 * Specifications for {@link FileSystemJobArchiverImpl}.
 *
 * @author tgianos
 */
class FileSystemJobArchiverImplSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder

    Path source
    Path target

    def setup() {
        this.source = temporaryFolder.newFolder().toPath()
        this.target = temporaryFolder.newFolder().toPath()

        // create a directory structure
        Files.write(this.source.resolve("stdout"), UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
        Files.write(this.source.resolve("stderr"), UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
        def genieDir = Files.createDirectory(this.source.resolve("genie"))
        Files.write(genieDir.resolve("env.sh"), UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
        def genieSubDir = Files.createDirectory(genieDir.resolve("subdir"))
        Files.write(genieSubDir.resolve("exit"), UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
        def clusterDir = Files.createDirectory(this.source.resolve("cluster"))
        Files.write(
            clusterDir.resolve("clusterSetupFile.sh"),
            UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)
        )
    }

    def "Can archive job directory"() {
        def archiver = new FileSystemJobArchiverImpl()
        def visitor = new DirectoryComparator(this.source, this.target)

        when: "We list the target directory"
        Stream<Path> paths = Files.list(this.target)

        then: "It is empty"
        paths.count() == 0

        when: "We archive the source to the target and compare the directories"
        archiver.archiveDirectory(this.source, this.target.toUri())
        Files.walkFileTree(this.source, visitor)
        def result = visitor.isSame()

        then:
        result
    }

    private static class DirectoryComparator extends SimpleFileVisitor<Path> {
        private final Path source
        private final Path target
        private boolean same = true

        DirectoryComparator(final Path source, final Path target) {
            this.source = source
            this.target = target
        }

        @Override
        FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            // make sure dir exists in target
            final Path targetDir = this.target.resolve(this.source.relativize(dir))
            if (!Files.exists(targetDir) || !Files.isDirectory(targetDir)) {
                this.same = false
                return FileVisitResult.TERMINATE
            }

            return FileVisitResult.CONTINUE
        }

        @Override
        FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            // make sure file exists in target
            final Path targetFile = this.target.resolve(this.source.relativize(file))
            if (!Files.exists(targetFile) || !Files.isRegularFile(targetFile)) {
                this.same = false
                return FileVisitResult.TERMINATE
            }

            // Compare contents
            String sourceMD5 = file.withInputStream {
                stream -> DigestUtils.md5Hex(stream)
            }
            String targetMD5 = targetFile.withInputStream {
                stream -> DigestUtils.md5Hex(stream)
            }
            if (sourceMD5 != targetMD5) {
                this.same = false
                return FileVisitResult.TERMINATE
            }

            return FileVisitResult.CONTINUE
        }

        boolean isSame() {
            return this.same
        }
    }
}
