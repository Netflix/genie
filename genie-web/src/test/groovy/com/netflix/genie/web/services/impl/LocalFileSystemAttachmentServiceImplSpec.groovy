/*
 *
 *  Copyright 2020 Netflix, Inc.
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
import com.netflix.genie.web.exceptions.checked.IllegalAttachmentFileNameException
import com.netflix.genie.web.exceptions.checked.AttachmentTooLargeException
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException
import com.netflix.genie.web.properties.AttachmentServiceProperties
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.RandomStringUtils
import org.mockito.Mockito
import org.springframework.core.io.FileSystemResource
import org.springframework.core.io.Resource
import org.springframework.util.unit.DataSize
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.stream.Collectors

class LocalFileSystemAttachmentServiceImplSpec extends Specification {

    AttachmentServiceProperties serviceProperties
    LocalFileSystemAttachmentServiceImpl service

    @TempDir
    Path temporaryFolder


    def setup() {
        this.serviceProperties = new AttachmentServiceProperties()
        this.serviceProperties.setLocationPrefix(this.temporaryFolder.toUri())
        this.serviceProperties.setMaxSize(DataSize.ofBytes(100))
        this.serviceProperties.setMaxTotalSize(DataSize.ofBytes(150))
        this.service = Mockito.spy(new LocalFileSystemAttachmentServiceImpl(serviceProperties))
    }

    def "saveAttachments with no attachments"() {
        Set<Resource> attachments = Sets.newHashSet()

        when:
        Set<URI> attachmentUris = service.saveAttachments(null, attachments)

        then:
        attachmentUris.isEmpty()
    }

    @Unroll
    def "saveAttachments (job id present: #jobIdPresent)"() {
        File input1 = Files.createFile(this.temporaryFolder.resolve("file1.txt")).toFile()
        File input2 = Files.createFile(this.temporaryFolder.resolve("file2.txt")).toFile()
        input1.write(RandomStringUtils.randomAscii(50))
        input2.write(RandomStringUtils.randomAscii(80))
        Resource resource1 = new FileSystemResource(input1)
        Resource resource2 = new FileSystemResource(input2)
        Set<Resource> attachments = Sets.newHashSet(resource1, resource2)

        String jobId = null
        if (jobIdPresent) {
            jobId = UUID.randomUUID().toString()
        }

        when:
        Set<URI> attachmentUris = service.saveAttachments(jobId, attachments)

        then:
        attachmentUris.size() == 2
        List<String> attachmentsList = attachmentUris.stream().map({ uri -> Paths.get(uri).toAbsolutePath().toString() }).collect(Collectors.toList())
        Collections.sort(attachmentsList)
        FileUtils.contentEquals(new File(attachmentsList.get(0)), input1)
        FileUtils.contentEquals(new File(attachmentsList.get(1)), input2)

        where:
        jobIdPresent << [true, false]
    }

    @Unroll
    def "reject attachments with sizes: #firstFileSize and #secondFileSize"() {
        File input1 = Files.createFile(this.temporaryFolder.resolve("file1.txt")).toFile()
        File input2 = Files.createFile(this.temporaryFolder.resolve("file2.txt")).toFile()
        input1.write(RandomStringUtils.randomAscii(firstFileSize))
        input2.write(RandomStringUtils.randomAscii(secondFileSize))
        Resource resource1 = new FileSystemResource(input1)
        Resource resource2 = new FileSystemResource(input2)
        Set<Resource> attachments = Sets.newHashSet(resource1, resource2)

        when:
        service.saveAttachments(null, attachments)

        then:
        thrown(AttachmentTooLargeException)


        where:
        firstFileSize | secondFileSize
        110           | 10
        10            | 110
        100           | 100
    }

    def "saveAttachments copy exception (job id present: #jobIdPresent)"() {
        File input = Files.createFile(this.temporaryFolder.resolve("file1.txt")).toFile()
        input.write(RandomStringUtils.randomAscii(50))
        Resource resource = new FileSystemResource(input)
        Set<Resource> attachments = Sets.newHashSet(resource)
        input.delete()

        when:
        service.saveAttachments(null, attachments)

        then:
        thrown(SaveAttachmentException)
    }

    def "saveAttachments create base directory exception"() {
        this.serviceProperties.setLocationPrefix(Paths.get("/likely-fail-to-create/genie/attachments").toUri())

        when:
        this.service = new LocalFileSystemAttachmentServiceImpl(serviceProperties)

        then:
        thrown(IOException)
    }

    def "saveAttachments create attachments directory exception"() {
        File base = new File(serviceProperties.getLocationPrefix())
        FileUtils.deleteDirectory(base)
        base.createNewFile()
        Set<Resource> attachments = Sets.newHashSet(Mock(Resource))

        when:
        service.saveAttachments(null, attachments)

        then:
        thrown(SaveAttachmentException)
    }

    def "reject attachments with illegal filename containing /"() {
        Set<Resource> attachments = new HashSet<Resource>()
        Resource attachment = Mockito.mock(Resource.class)
        Mockito.doReturn("../../../root/breakout.file").when(attachment).getFilename()
        attachments.add(attachment)

        when:
        service.saveAttachments(null, attachments)

        then:
        thrown(IllegalAttachmentFileNameException)
    }

    def "reject attachments with illegal filename containing \\"() {
        Set<Resource> attachments = new HashSet<Resource>()
        Resource attachment = Mockito.mock(Resource.class)
        Mockito.doReturn("c:\\root\\breakout.file").when(attachment).getFilename()
        attachments.add(attachment)

        when:
        service.saveAttachments(null, attachments)

        then:
        thrown(IllegalAttachmentFileNameException)
    }

    def "reject attachments with illegal filename is ."() {
        Set<Resource> attachments = new HashSet<Resource>()
        Resource attachment = Mockito.mock(Resource.class)
        Mockito.doReturn(".").when(attachment).getFilename()
        attachments.add(attachment)

        when:
        service.saveAttachments(null, attachments)

        then:
        thrown(IllegalAttachmentFileNameException)
    }

    def "reject attachments with illegal filename is .."() {
        Set<Resource> attachments = new HashSet<Resource>()
        Resource attachment = Mockito.mock(Resource.class)
        Mockito.doReturn("..").when(attachment).getFilename()
        attachments.add(attachment)

        when:
        service.saveAttachments(null, attachments)

        then:
        thrown(IllegalAttachmentFileNameException)
    }

    def "reject attachments with illegal filename for base path check"() {
        Set<Resource> attachments = new HashSet<Resource>()
        Resource attachment = Mockito.mock(Resource.class)
        Mockito.doReturn("file1.text").when(attachment).getFilename()
        File file = Mockito.mock(File.class)
        Mockito.doReturn("/dummy").when(file).getCanonicalPath()
        Mockito.doReturn(file).when(service).createTempFile(Mockito.anyString(), Mockito.anyString());
        attachments.add(attachment)

        when:
        service.saveAttachments(null, attachments)

        then:
        thrown(IllegalAttachmentFileNameException)
    }
}
