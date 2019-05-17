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
package com.netflix.genie.common.internal.dto

import com.netflix.genie.common.util.GenieObjectMapper
import org.apache.commons.codec.digest.DigestUtils
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification
import spock.lang.Unroll

import javax.annotation.Nullable
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes

/**
 * Specifications for {@link DirectoryManifest}.
 *
 * @author tgianos
 */
class DirectoryManifestSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder = new TemporaryFolder()
    Path rootPath
    String root
    Path stdoutPath
    String stdout
    Path stderrPath
    String stderr
    Path genieDirPath
    String genieDir
    Path genieSubDirPath
    String genieSubDir
    Path envFilePath
    String envFile
    Path exitFilePath
    String exitFile
    Path clusterDirPath
    String clusterDir
    Path clusterSetupScriptPath
    String clusterSetupScript
    Path applicationDirPath
    String applicationDir
    Path applicationSetupScriptPath
    String applicationSetupScript
    Path symLinkFileRealPath
    Path symLinkFilePath
    String symLinkFile
    Path cyclicSymLinkPath
    String cyclicSymLink
    Path stdOutSymLinkPath
    String stdOutSymLink

    long sizeOfFiles

    def setup() {
        this.rootPath = this.temporaryFolder.newFolder().toPath()

        // Valid symlink to outside of job directory
        this.symLinkFileRealPath = this.temporaryFolder.newFile("realSymFile").toPath()
        Files.write(this.symLinkFileRealPath, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
        this.symLinkFilePath = Files.createSymbolicLink(
            this.rootPath.resolve("symFile"), this.symLinkFileRealPath
        )

        // create a directory structure
        this.stdoutPath = Files.write(
            this.rootPath.resolve("stdout"),
            UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)
        )
        this.stdOutSymLinkPath = Files.createSymbolicLink(this.rootPath.resolve("stdOutSymLink"), this.stdoutPath)
        this.stderrPath = Files.write(
            this.rootPath.resolve("stderr"),
            UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)
        )
        this.genieDirPath = Files.createDirectory(this.rootPath.resolve("genie"))
        this.envFilePath = Files.write(
            this.genieDirPath.resolve("env.sh"),
            "#!/bin/bash".getBytes(StandardCharsets.UTF_8)
        )
        this.genieSubDirPath = Files.createDirectory(this.genieDirPath.resolve("subdir"))
        this.exitFilePath = Files.write(
            this.genieSubDirPath.resolve("exit"),
            UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)
        )
        this.clusterDirPath = Files.createDirectory(this.rootPath.resolve("cluster"))
        this.clusterSetupScriptPath = Files.write(
            this.clusterDirPath.resolve("clusterSetupFile.sh"),
            "#!/bin/bash\necho \"hello world!\"".getBytes(StandardCharsets.UTF_8)
        )

        this.applicationDirPath = Files.createDirectory(this.rootPath.resolve("application"))
        this.applicationSetupScriptPath = Files.write(
            this.applicationDirPath.resolve("applicationSetupFile.sh"),
            "#!/bin/bash\necho \"hello world!\"".getBytes(StandardCharsets.UTF_8)
        )

        // Cyclic symlink
        this.cyclicSymLinkPath = Files.createSymbolicLink(this.genieDirPath.resolve("testCycle"), this.genieDirPath)

        // Get the relative paths
        this.root = this.rootPath.relativize(this.rootPath).toString()
        this.stdout = this.rootPath.relativize(this.stdoutPath).toString()
        this.stderr = this.rootPath.relativize(this.stderrPath).toString()
        this.genieDir = this.rootPath.relativize(this.genieDirPath).toString()
        this.envFile = this.rootPath.relativize(this.envFilePath).toString()
        this.genieSubDir = this.rootPath.relativize(this.genieSubDirPath).toString()
        this.exitFile = this.rootPath.relativize(this.exitFilePath).toString()
        this.clusterDir = this.rootPath.relativize(this.clusterDirPath).toString()
        this.clusterSetupScript = this.rootPath.relativize(this.clusterSetupScriptPath)
        this.applicationDir = this.rootPath.relativize(this.applicationDirPath)
        this.applicationSetupScript = this.rootPath.relativize(this.applicationSetupScriptPath)
        this.symLinkFile = this.rootPath.relativize(this.symLinkFilePath)
        this.stdOutSymLink = this.rootPath.relativize(this.stdOutSymLinkPath)
        this.cyclicSymLink = this.rootPath.relativize(this.cyclicSymLinkPath)

        // Calculate the file sizes
        this.sizeOfFiles = 0L
        [
            this.stdoutPath,
            this.stderrPath,
            this.envFilePath,
            this.exitFilePath,
            this.clusterSetupScriptPath,
            this.applicationSetupScriptPath,
            this.symLinkFileRealPath,
            this.stdoutPath // In here twice to account for symlink to this file
        ].forEach({ this.sizeOfFiles += Files.size(it) })
    }

    @Unroll
    def "can create a manifest, serialize it, deserialize it and verify correctness (md5: #includeMd5)"() {
        when:
        def manifest = new DirectoryManifest(this.rootPath, includeMd5, new DirectoryManifest.Filter() { })
        def json = GenieObjectMapper.getMapper().writeValueAsString(manifest)
        def manifest2 = GenieObjectMapper.getMapper().readValue(json, DirectoryManifest.class)

        then:
        manifest == manifest2
        verifyManifest(manifest, includeMd5)
        verifyManifest(manifest2, includeMd5)

        where:
        includeMd5 | _
        true       | _
        false      | _
    }

    def "can create a manifest with filter"() {
        when:
        def manifest = new DirectoryManifest(this.rootPath, false, new DirectoryManifest.Filter() {
            @Override
            boolean includeFile(final Path filePath, final BasicFileAttributes attrs) {
                return filePath.getFileName().toString() != "env.sh"
            }

            @Override
            boolean includeDirectory(final Path dirPath, final BasicFileAttributes attrs) {
                return dirPath.getFileName().toString() != "cluster"
            }

            @Override
            boolean walkDirectory(final Path dirPath, final BasicFileAttributes attrs) {
                return dirPath.getFileName().toString() != "application"
            }
        })

        then:
        manifest.getEntries().size() == 9
        manifest.getFiles().size() == 5
        manifest.getNumFiles() == 5
        manifest.getDirectories().size() == 4
        manifest.getNumDirectories() == 4
        manifest.hasEntry(this.root)
        manifest.hasEntry(this.stdout)
        manifest.hasEntry(this.stderr)
        manifest.hasEntry(this.genieDir)
        !manifest.hasEntry(this.envFile)
        manifest.hasEntry(this.genieSubDir)
        manifest.hasEntry(this.exitFile)
        !manifest.hasEntry(this.clusterDir)
        !manifest.hasEntry(this.clusterSetupScript)
        manifest.hasEntry(this.applicationDir)
        !manifest.hasEntry(this.applicationSetupScript)
        manifest.hasEntry(this.symLinkFile)
        manifest.hasEntry(this.stdOutSymLink)
    }

    void verifyManifest(DirectoryManifest manifest, boolean expectMd5Present) {
        assert manifest.getEntries().size() == 13
        assert manifest.getFiles().size() == 8
        assert manifest.getNumFiles() == 8
        assert manifest.getDirectories().size() == 5
        assert manifest.getNumDirectories() == 5
        assert manifest.hasEntry(this.root)
        assert manifest.hasEntry(this.stdout)
        assert manifest.hasEntry(this.stderr)
        assert manifest.hasEntry(this.genieDir)
        assert manifest.hasEntry(this.envFile)
        assert manifest.hasEntry(this.genieSubDir)
        assert manifest.hasEntry(this.exitFile)
        assert manifest.hasEntry(this.clusterDir)
        assert manifest.hasEntry(this.clusterSetupScript)
        assert manifest.hasEntry(this.applicationDir)
        assert manifest.hasEntry(this.applicationSetupScript)
        assert manifest.hasEntry(this.symLinkFile)
        assert manifest.hasEntry(this.stdOutSymLink)
        // This is to document behavior
        assert !manifest.hasEntry(this.cyclicSymLink)

        this.verifyEntry(
            manifest.getEntry(this.root).orElseThrow({ new IllegalArgumentException() }),
            this.root,
            this.rootPath,
            true,
            null,
            [this.stderr, this.stdout, this.genieDir, this.clusterDir, this.applicationDir, this.symLinkFile, this.stdOutSymLink],
            false
        )
        this.verifyEntry(
            manifest.getEntry(this.stdout).orElseThrow({ new IllegalArgumentException() }),
            this.stdout,
            this.stdoutPath,
            false,
            this.root,
            [],
            expectMd5Present
        )
        this.verifyEntry(
            manifest.getEntry(this.stderr).orElseThrow({ new IllegalArgumentException() }),
            this.stderr,
            this.stderrPath,
            false,
            this.root,
            [],
            expectMd5Present
        )
        this.verifyEntry(
            manifest.getEntry(this.genieDir).orElseThrow({ new IllegalArgumentException() }),
            this.genieDir,
            this.genieDirPath,
            true,
            this.root,
            // Note: the cyclic link will be listed as a child but not have an entry ATM probably resulting in 404
            //       at runtime but not sure what we can do given how the children are determined today without
            //       launching recursive visitors
            [this.envFile, this.genieSubDir, this.cyclicSymLink],
            false
        )
        this.verifyEntry(
            manifest.getEntry(this.envFile).orElseThrow({ new IllegalArgumentException() }),
            this.envFile,
            this.envFilePath,
            false,
            this.genieDir,
            [],
            expectMd5Present
        )
        this.verifyEntry(
            manifest.getEntry(this.genieSubDir).orElseThrow({ new IllegalArgumentException() }),
            this.genieSubDir,
            this.genieSubDirPath,
            true,
            this.genieDir,
            [this.exitFile],
            false
        )
        this.verifyEntry(
            manifest.getEntry(this.exitFile).orElseThrow({ new IllegalArgumentException() }),
            this.exitFile,
            this.exitFilePath,
            false,
            this.genieSubDir,
            [],
            expectMd5Present
        )
        this.verifyEntry(
            manifest.getEntry(this.clusterDir).orElseThrow({ new IllegalArgumentException() }),
            this.clusterDir,
            this.clusterDirPath,
            true,
            this.root,
            [this.clusterSetupScript],
            false
        )
        this.verifyEntry(
            manifest.getEntry(this.clusterSetupScript).orElseThrow({ new IllegalArgumentException() }),
            this.clusterSetupScript,
            this.clusterSetupScriptPath,
            false,
            this.clusterDir,
            [],
            expectMd5Present
        )
        this.verifyEntry(
            manifest.getEntry(this.applicationDir).orElseThrow({ new IllegalArgumentException() }),
            this.applicationDir,
            this.applicationDirPath,
            true,
            this.root,
            [this.applicationSetupScript],
            false
        )
        this.verifyEntry(
            manifest.getEntry(this.applicationSetupScript).orElseThrow({ new IllegalArgumentException() }),
            this.applicationSetupScript,
            this.applicationSetupScriptPath,
            false,
            this.applicationDir,
            [],
            expectMd5Present
        )
        this.verifyEntry(
            manifest.getEntry(this.symLinkFile).orElseThrow({ new IllegalArgumentException() }),
            this.symLinkFile,
            this.symLinkFilePath,
            false,
            this.root,
            [],
            expectMd5Present
        )
        this.verifyEntry(
            manifest.getEntry(this.stdOutSymLink).orElseThrow({ new IllegalArgumentException() }),
            this.stdOutSymLink,
            this.stdOutSymLinkPath,
            false,
            this.root,
            [],
            expectMd5Present
        )

        assert manifest.getFiles().contains(manifest.getEntry(this.stdout).orElse(null))
        assert manifest.getFiles().contains(manifest.getEntry(this.stderr).orElse(null))
        assert manifest.getFiles().contains(manifest.getEntry(this.envFile).orElse(null))
        assert manifest.getFiles().contains(manifest.getEntry(this.exitFile).orElse(null))
        assert manifest.getFiles().contains(manifest.getEntry(this.clusterSetupScript).orElse(null))
        assert manifest.getFiles().contains(manifest.getEntry(this.applicationSetupScript).orElse(null))
        assert manifest.getFiles().contains(manifest.getEntry(this.symLinkFile).orElse(null))

        assert manifest
            .getDirectories()
            .contains(manifest.getEntry(this.root).orElse(null))
        assert manifest
            .getDirectories()
            .contains(manifest.getEntry(this.genieDir).orElse(null))
        assert manifest
            .getDirectories()
            .contains(manifest.getEntry(this.genieSubDir).orElse(null))
        assert manifest
            .getDirectories()
            .contains(manifest.getEntry(this.clusterDir).orElse(null))
        assert manifest
            .getDirectories()
            .contains(manifest.getEntry(this.applicationDir).orElse(null))

        assert !manifest.getEntry(UUID.randomUUID().toString()).isPresent()

        assert manifest.getTotalSizeOfFiles() == this.sizeOfFiles
    }

    def verifyEntry(
        DirectoryManifest.ManifestEntry entry,
        String expectedRelativePath,
        Path expectedFile,
        boolean isDirectory,
        @Nullable String expectedParent,
        Collection<String> expectedChildren,
        boolean expectedMd5Present
    ) {
        assert entry.getPath() == expectedRelativePath
        assert entry.getName() == expectedFile.getFileName().toString()
        assert entry.getSize() == Files.size(expectedFile)
        if (isDirectory) {
            assert entry.isDirectory()
            assert !entry.getMd5().isPresent()
            assert !entry.getMimeType().isPresent()
        } else {
            assert !entry.isDirectory()
            if (expectedMd5Present) {
                assert entry
                    .getMd5()
                    .orElseThrow({ new IllegalArgumentException() }) == expectedFile.withInputStream {
                    return DigestUtils.md5Hex(it)
                }
            } else {
                assert !entry.getMd5().isPresent()
            }
            assert entry.getMimeType().isPresent()
        }
        if (expectedParent == null) {
            assert !entry.getParent().isPresent()
        } else {
            assert entry.getParent().orElseThrow({ new IllegalArgumentException() }) == expectedParent
        }
        assert entry.getChildren().size() == expectedChildren.size()
        assert entry.getChildren().containsAll(expectedChildren)
    }
}
