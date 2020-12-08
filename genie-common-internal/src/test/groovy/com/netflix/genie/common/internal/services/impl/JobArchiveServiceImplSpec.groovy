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

import com.netflix.genie.common.external.util.GenieObjectMapper
import com.netflix.genie.common.internal.dtos.DirectoryManifest
import com.netflix.genie.common.internal.services.JobArchiveService
import com.netflix.genie.common.internal.services.JobArchiver
import org.apache.commons.lang3.StringUtils
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

/**
 * Specifications for {@link JobArchiveServiceImpl}.
 *
 * @author tgianos
 */
class JobArchiveServiceImplSpec extends Specification {

    @TempDir
    Path temporaryFolder

    def "When archiveDirectory is invoked a valid manifest is written into the expected directory"() {
        def skippedArchiver = Mock(JobArchiver)
        def archiver = Mock(JobArchiver)
        DirectoryManifest.Factory directoryManifestFactory = Mock(DirectoryManifest.Factory)
        def service = new JobArchiveServiceImpl([skippedArchiver, archiver], directoryManifestFactory)
        def jobDirectory = Files.createDirectory(this.temporaryFolder.resolve(UUID.randomUUID().toString()))
        def someFilePath = jobDirectory.resolve("someFile")
        Files.write(someFilePath, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
        Files.createDirectory(jobDirectory.resolve("subDir"))
        def target = Files.createDirectory(this.temporaryFolder.resolve(UUID.randomUUID().toString())).toUri()
        def manifestDirectoryPath = StringUtils.isBlank(JobArchiveService.MANIFEST_DIRECTORY)
            ? jobDirectory
            : jobDirectory.resolve(JobArchiveService.MANIFEST_DIRECTORY)
        def manifestPath = manifestDirectoryPath.resolve(JobArchiveService.MANIFEST_NAME)
        def originalManifest = new DirectoryManifest.Factory().getDirectoryManifest(jobDirectory, true)
        def filesList = [manifestPath, someFilePath].stream().map({ path -> path.toFile() }).collect(Collectors.toList())

        when:
        service.archiveDirectory(jobDirectory, target)

        then:
        1 * directoryManifestFactory.getDirectoryManifest(jobDirectory, true) >> originalManifest
        Files.exists(manifestPath)
        1 * skippedArchiver.archiveDirectory(jobDirectory, filesList, target) >> false
        1 * archiver.archiveDirectory(jobDirectory, filesList, target) >> true

        when:
        def manifest = GenieObjectMapper.getMapper().readValue(manifestPath.toFile(), DirectoryManifest)

        then:
        manifest.getNumDirectories() == 2
        manifest.getNumFiles() == 1
        manifest == originalManifest
    }
}
