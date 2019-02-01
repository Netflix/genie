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

import javax.annotation.Nullable
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

/**
 * Specifications for {@link JobDirectoryManifest}.
 *
 * @author tgianos
 */
class JobDirectoryManifestSpec extends Specification {

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
    long sizeOfFiles

    def setup() {
        this.rootPath = this.temporaryFolder.newFolder().toPath()

        // create a directory structure
        this.stdoutPath = Files.write(
            this.rootPath.resolve("stdout"),
            UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)
        )
        this.stderrPath = Files.write(
            this.rootPath.resolve("stderr"),
            UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)
        )
        this.genieDirPath = Files.createDirectory(this.rootPath.resolve("genie"))
        this.envFilePath = Files.write(
            this.genieDirPath.resolve("env.sh"),
            "#!/bin/bash".getBytes(StandardCharsets.UTF_8)
        )
        this.genieSubDirPath = Files.createDirectory(genieDirPath.resolve("subdir"))
        this.exitFilePath = Files.write(
            this.genieSubDirPath.resolve("exit"),
            UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)
        )
        this.clusterDirPath = Files.createDirectory(this.rootPath.resolve("cluster"))
        this.clusterSetupScriptPath = Files.write(
            this.clusterDirPath.resolve("clusterSetupFile.sh"),
            "#!/bin/bash\necho \"hello world!\"".getBytes(StandardCharsets.UTF_8)
        )

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

        // Calculate the file sizes
        this.sizeOfFiles = 0L
        [this.stdoutPath, this.stderrPath, this.envFilePath, this.exitFilePath, this.clusterSetupScriptPath]
            .forEach({ this.sizeOfFiles += Files.size(it) })
    }

    def "can create a manifest, serialize it, deserialize it and verify correctness"() {
        when:
        def manifest = new JobDirectoryManifest(this.rootPath)
        def json = GenieObjectMapper.getMapper().writeValueAsString(manifest)
        def manifest2 = GenieObjectMapper.getMapper().readValue(json, JobDirectoryManifest.class)

        then:
        manifest == manifest2
        verifyManifest(manifest)
        verifyManifest(manifest2)
    }

    void verifyManifest(JobDirectoryManifest manifest) {
        assert manifest.getEntries().size() == 9
        assert manifest.getFiles().size() == 5
        assert manifest.getNumFiles() == 5
        assert manifest.getDirectories().size() == 4
        assert manifest.getNumDirectories() == 4
        assert manifest.hasEntry(this.root)
        assert manifest.hasEntry(this.stdout)
        assert manifest.hasEntry(this.stderr)
        assert manifest.hasEntry(this.genieDir)
        assert manifest.hasEntry(this.envFile)
        assert manifest.hasEntry(this.genieSubDir)
        assert manifest.hasEntry(this.exitFile)
        assert manifest.hasEntry(this.clusterDir)
        assert manifest.hasEntry(this.clusterSetupScript)

        this.verifyEntry(
            manifest.getEntry(this.root).orElseThrow({ new IllegalArgumentException() }),
            this.root,
            this.rootPath,
            true,
            null,
            [this.stderr, this.stdout, this.genieDir, this.clusterDir]
        )
        this.verifyEntry(
            manifest.getEntry(this.stdout).orElseThrow({ new IllegalArgumentException() }),
            this.stdout,
            this.stdoutPath,
            false,
            this.root,
            []
        )
        this.verifyEntry(
            manifest.getEntry(this.stderr).orElseThrow({ new IllegalArgumentException() }),
            this.stderr,
            this.stderrPath,
            false,
            this.root,
            []
        )
        this.verifyEntry(
            manifest.getEntry(this.genieDir).orElseThrow({ new IllegalArgumentException() }),
            this.genieDir,
            this.genieDirPath,
            true,
            this.root,
            [this.envFile, this.genieSubDir]
        )
        this.verifyEntry(
            manifest.getEntry(this.envFile).orElseThrow({ new IllegalArgumentException() }),
            this.envFile,
            this.envFilePath,
            false,
            this.genieDir,
            []
        )
        this.verifyEntry(
            manifest.getEntry(this.genieSubDir).orElseThrow({ new IllegalArgumentException() }),
            this.genieSubDir,
            this.genieSubDirPath,
            true,
            this.genieDir,
            [this.exitFile]
        )
        this.verifyEntry(
            manifest.getEntry(this.exitFile).orElseThrow({ new IllegalArgumentException() }),
            this.exitFile,
            this.exitFilePath,
            false,
            this.genieSubDir,
            []
        )
        this.verifyEntry(
            manifest.getEntry(this.clusterDir).orElseThrow({ new IllegalArgumentException() }),
            this.clusterDir,
            this.clusterDirPath,
            true,
            this.root,
            [this.clusterSetupScript]
        )
        this.verifyEntry(
            manifest.getEntry(this.clusterSetupScript).orElseThrow({ new IllegalArgumentException() }),
            this.clusterSetupScript,
            this.clusterSetupScriptPath,
            false,
            this.clusterDir,
            []
        )

        assert manifest.getFiles().contains(manifest.getEntry(this.stdout).orElse(null))
        assert manifest.getFiles().contains(manifest.getEntry(this.stderr).orElse(null))
        assert manifest.getFiles().contains(manifest.getEntry(this.envFile).orElse(null))
        assert manifest.getFiles().contains(manifest.getEntry(this.exitFile).orElse(null))
        assert manifest.getFiles().contains(manifest.getEntry(this.clusterSetupScript).orElse(null))

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

        assert !manifest.getEntry(UUID.randomUUID().toString()).isPresent()

        assert manifest.getTotalSizeOfFiles() == this.sizeOfFiles
    }

    def verifyEntry(
        JobDirectoryManifest.ManifestEntry entry,
        String expectedRelativePath,
        Path expectedFile,
        boolean isDirectory,
        @Nullable String expectedParent,
        Collection<String> expectedChildren
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
            assert entry
                .getMd5()
                .orElseThrow({ new IllegalArgumentException() }) == expectedFile.withInputStream {
                return DigestUtils.md5Hex(it)
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
