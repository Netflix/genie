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

import com.netflix.genie.common.internal.aws.s3.S3TransferManagerFactory
import com.netflix.genie.common.internal.exceptions.checked.JobArchiveException
import software.amazon.awssdk.http.SdkHttpResponse
import software.amazon.awssdk.services.s3.S3Uri
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload
import software.amazon.awssdk.transfer.s3.model.FileUpload
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

/**
 * Specifications for {@link S3JobArchiverImpl}.
 *
 * @author standon
 */
class S3JobArchiverImplSpec extends Specification {
    @TempDir
    Path temporaryFolder
    S3TransferManagerFactory s3TransferManagerFactory
    S3TransferManager transferManager
    S3JobArchiverImpl s3ArchivalService

    File jobDir
    String bucketName = UUID.randomUUID().toString()
    String baseLocation = "genie/main/logs"
    File stdout
    File stderr
    File run
    File genieDir
    File applicationsDir

    File sparkDir
    File clustersDir
    File hadoopH2Dir
    File hadoopH2DirConfig
    File hadoopCoreSite
    File commandsDir
    File sparkShellSetUp
    File logsDir
    File logFile
    List<File> allFiles

    URI archivalLocationURI
    S3Uri s3Uri

    void setup() {
        this.s3TransferManagerFactory = Mock(S3TransferManagerFactory)
        this.transferManager = Mock(S3TransferManager)
        this.s3Uri = Mock(S3Uri)
        this.s3ArchivalService = new S3JobArchiverImpl(this.s3TransferManagerFactory)
        this.jobDir = Files.createDirectory(this.temporaryFolder.resolve(UUID.randomUUID().toString())).toFile()
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

        this.archivalLocationURI = new URI(
            "s3://" + bucketName + File.separator + baseLocation + File.separator + jobDir.getName()
        )

        this.allFiles = [
            this.stdout,
            this.stderr,
            this.run,
            this.hadoopCoreSite,
            this.sparkShellSetUp,
            this.logFile
        ]
    }

    def "Archiving a job folder defers to the S3 Transfer Manager returned by the factory"() {
        given:
        def fileUpload = Mock(FileUpload)
        def completedFileUpload = Mock(CompletedFileUpload)
        def putObjectResponse = Mock(PutObjectResponse)
        def sdkHttpResponse = Mock(SdkHttpResponse)
        def completableFuture = CompletableFuture.completedFuture(completedFileUpload)

        when:
        def result = this.s3ArchivalService.archiveDirectory(this.jobDir.toPath(), this.allFiles, this.archivalLocationURI)

        then:
        1 * this.s3TransferManagerFactory.getS3Uri(this.archivalLocationURI) >> this.s3Uri
        1 * this.s3Uri.bucket() >> Optional.of(this.bucketName)
        1 * this.s3Uri.key() >> Optional.of(this.baseLocation)
        1 * this.s3TransferManagerFactory.getTransferManager(this.s3Uri) >> this.transferManager

        this.allFiles.size() * this.transferManager.uploadFile(_ as UploadFileRequest) >> fileUpload
        this.allFiles.size() * fileUpload.completionFuture() >> completableFuture

        _ * completedFileUpload.response() >> putObjectResponse
        _ * putObjectResponse.sdkHttpResponse() >> sdkHttpResponse
        _ * sdkHttpResponse.isSuccessful() >> true
        _ * putObjectResponse.eTag() >> "etag-value"

        result
    }

    def "If there is a failure during upload, the method returns false"() {
        given:
        def fileUpload = Mock(FileUpload)
        def completedFileUpload = Mock(CompletedFileUpload)
        def putObjectResponse = Mock(PutObjectResponse)
        def sdkHttpResponse = Mock(SdkHttpResponse)
        def completableFuture = CompletableFuture.completedFuture(completedFileUpload)

        when:
        def result = this.s3ArchivalService.archiveDirectory(this.jobDir.toPath(), this.allFiles, this.archivalLocationURI)

        then:
        1 * this.s3TransferManagerFactory.getS3Uri(this.archivalLocationURI) >> this.s3Uri
        1 * this.s3Uri.bucket() >> Optional.of(this.bucketName)
        1 * this.s3Uri.key() >> Optional.of(this.baseLocation)
        1 * this.s3TransferManagerFactory.getTransferManager(this.s3Uri) >> this.transferManager

        this.allFiles.size() * this.transferManager.uploadFile(_ as UploadFileRequest) >> fileUpload
        this.allFiles.size() * fileUpload.completionFuture() >> completableFuture

        _ * completedFileUpload.response() >> putObjectResponse
        _ * putObjectResponse.sdkHttpResponse() >> sdkHttpResponse

        1 * sdkHttpResponse.isSuccessful() >> false
        _ * sdkHttpResponse.isSuccessful() >> true

        _ * sdkHttpResponse.statusText() >> Optional.of("Failed")
        _ * putObjectResponse.eTag() >> "etag-value"

        !result
    }

    def "Archival Exception thrown if there is error archiving"() {
        when:
        this.s3ArchivalService.archiveDirectory(this.jobDir.toPath(), this.allFiles, this.archivalLocationURI)

        then:
        1 * this.s3TransferManagerFactory.getS3Uri(this.archivalLocationURI) >> { throw new S3Exception("test") }
        thrown(JobArchiveException)
    }

    def "If bucket is missing in URI, an exception is thrown"() {
        when:
        this.s3ArchivalService.archiveDirectory(this.jobDir.toPath(), this.allFiles, this.archivalLocationURI)

        then:
        1 * this.s3TransferManagerFactory.getS3Uri(this.archivalLocationURI) >> this.s3Uri
        1 * this.s3Uri.bucket() >> Optional.empty()
        thrown(JobArchiveException)
    }
}
