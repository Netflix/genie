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

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.transfer.MultipleFileUpload
import com.amazonaws.services.s3.transfer.TransferManager
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory
import com.netflix.genie.common.internal.exceptions.checked.JobArchiveException
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

/**
 * Specifications for {@link S3JobArchiverImpl}.
 *
 * @author standon
 */
class S3JobArchiverImplSpec extends Specification {
    @Rule
    TemporaryFolder temporaryFolder
    S3ClientFactory s3ClientFactory
    TransferManager transferManager
    S3JobArchiverImpl s3ArchivalService

    File jobDir
    String bucketName = UUID.randomUUID().toString()
    String baseLocation = "genie/main/logs"
    File stdout
    File stderr
    File run
    File genieDir
    File applicationsDir
    //Empty dir
    File sparkDir
    File clustersDir
    File hadoopH2Dir
    File hadoopH2DirConfig
    File hadoopCoreSite
    File commandsDir
    File sparkShellSetUp
    File logsDir
    File logFile

    AmazonS3URI archivalLocationS3URI

    void setup() {
        this.s3ClientFactory = Mock(S3ClientFactory)
        this.transferManager = Mock(TransferManager)
        this.s3ArchivalService = new S3JobArchiverImpl(this.s3ClientFactory)
        this.jobDir = this.temporaryFolder.newFolder()
        this.stdout = new File(jobDir, "stdout")
        this.stdout.createNewFile()
        this.stdout.write("Stdout content")

        this.stderr = new File(this.jobDir, "stderr")
        this.stderr.createNewFile()
        this.stderr.write("Stderr content")

        this.run = new File(this.jobDir, "run")
        this.run.createNewFile()
        this.run.write("Run file content")

        this.genieDir = new File(this.jobDir, "genie")
        this.applicationsDir = new File(this.genieDir, "applications")
        this.applicationsDir.mkdirs()

        //empty dir
        this.sparkDir = new File(this.applicationsDir, "spark")
        this.sparkDir.mkdirs()

        this.clustersDir = new File(this.genieDir, "clusters")
        this.clustersDir.mkdirs()

        this.hadoopH2Dir = new File(this.clustersDir, "hadoopH2")
        this.hadoopH2Dir.mkdirs()

        this.hadoopH2DirConfig = new File(this.hadoopH2Dir, "config")
        this.hadoopH2DirConfig.mkdirs()

        this.hadoopCoreSite = new File(this.hadoopH2DirConfig, "core")
        this.hadoopCoreSite.createNewFile()
        this.hadoopCoreSite.write("Hadoop core site")

        this.commandsDir = new File(this.genieDir, "commands")
        this.commandsDir.mkdirs()

        this.sparkShellSetUp = new File(this.commandsDir, "setup.sh")
        this.sparkShellSetUp.createNewFile()
        this.sparkShellSetUp.write("Spark shell set up")

        this.logsDir = new File(this.genieDir, "logs")
        this.logsDir.mkdirs()

        this.logFile = new File(this.logsDir, "genie.log")
        this.logFile.write("logs")

        this.archivalLocationS3URI = new AmazonS3URI(
            "s3://" + bucketName + File.separator + baseLocation + File.separator + jobDir.getName()
        )
    }

    def "Archiving a job folder defers to the S3 Transfer Manager returned by the factory"() {
        def upload = Mock(MultipleFileUpload)

        when:
        def result = this.s3ArchivalService.archiveDirectory(this.jobDir.toPath(), this.archivalLocationS3URI.getURI())

        then:
        1 * this.s3ClientFactory.getTransferManager(_ as AmazonS3URI) >> this.transferManager
        1 * this.transferManager.uploadDirectory(
            this.archivalLocationS3URI.getBucket(),
            this.archivalLocationS3URI.getKey(),
            this.jobDir,
            true
        ) >> upload
        1 * upload.waitForCompletion()
        result
    }

    def "If it is not a valid S3 URI archival is not attempted with this implementation"() {
        when:
        def result = this.s3ArchivalService.archiveDirectory(jobDir.toPath(), new URI("file://abc"))

        then:
        !result
    }

    def "Archival Exception thrown if there is error archiving"() {
        when:
        this.s3ArchivalService.archiveDirectory(this.jobDir.toPath(), this.archivalLocationS3URI.getURI())

        then:
        1 * this.s3ClientFactory.getTransferManager(_ as AmazonS3URI) >> this.transferManager
        1 * this.transferManager.uploadDirectory(
            this.archivalLocationS3URI.getBucket(),
            this.archivalLocationS3URI.getKey(),
            this.jobDir,
            true
        ) >> { throw new AmazonServiceException("test") }
        thrown(JobArchiveException)
    }
}

