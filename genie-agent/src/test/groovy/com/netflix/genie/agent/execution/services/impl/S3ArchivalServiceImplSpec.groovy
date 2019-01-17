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

package com.netflix.genie.agent.execution.services.impl

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.PutObjectRequest
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory
import com.netflix.genie.agent.execution.exceptions.ArchivalException
import com.netflix.genie.test.categories.UnitTest
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths

@Category(UnitTest)
class S3ArchivalServiceImplSpec extends Specification {
    @Rule
    TemporaryFolder temporaryFolder
    S3ClientFactory s3ClientFactory
    AmazonS3 amazonS3
    S3ArchivalServiceImpl s3ArchivalService

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
        s3ClientFactory = Mock()
        amazonS3 = Mock()
        s3ArchivalService = new S3ArchivalServiceImpl(s3ClientFactory)
        jobDir = temporaryFolder.newFolder("job")
        stdout = new File(jobDir, "stdout")
        stdout.createNewFile()
        stdout.write("Stdout content")

        stderr = new File(jobDir, "stderr")
        stderr.createNewFile()
        stderr.write("Stderr content")

        run = new File(jobDir, "run")
        run.createNewFile()
        run.write("Run file content")

        genieDir = new File(jobDir, "genie")
        applicationsDir = new File(genieDir, "applications")
        applicationsDir.mkdirs()

        //empty dir
        sparkDir = new File(applicationsDir, "spark")
        sparkDir.mkdirs()

        clustersDir = new File(genieDir, "clusters")
        clustersDir.mkdirs()

        hadoopH2Dir = new File(clustersDir, "hadoopH2")
        hadoopH2Dir.mkdirs()

        hadoopH2DirConfig = new File(hadoopH2Dir, "config")
        hadoopH2DirConfig.mkdirs()

        hadoopCoreSite = new File(hadoopH2DirConfig, "core")
        hadoopCoreSite.createNewFile()
        hadoopCoreSite.write("Hadoop core site")

        commandsDir = new File(genieDir, "commands")
        commandsDir.mkdirs()

        sparkShellSetUp = new File(commandsDir, "setup.sh")
        sparkShellSetUp.createNewFile()
        sparkShellSetUp.write("Spark shell set up")

        logsDir = new File(genieDir, "logs")
        logsDir.mkdirs()

        logFile = new File(logsDir, "genie.log")
        logFile.write("logs")

        archivalLocationS3URI = new AmazonS3URI(
            "s3://" + bucketName + File.separator + baseLocation + File.separator + jobDir.getName()
        )
    }

    void cleanup() {
    }

    def "Archive a sample job folder. S3 objects creation calls called for files and empty directories with correct keys"() {

        when:
        s3ArchivalService.archive(jobDir.toPath(), archivalLocationS3URI.getURI())

        then:
        1 * s3ClientFactory.getClient(archivalLocationS3URI) >> amazonS3
        7 * amazonS3.putObject(_ as PutObjectRequest) >> {
            PutObjectRequest putObjectRequest ->
                assert putObjectRequest.getBucketName() == bucketName

                //Name of the file for which the s3 putObject call is invoked
                String fileName =
                    putObjectRequest.getFile() == null ?
                        sparkDir.getName() :
                        putObjectRequest.getFile().getName()

                switch (fileName) {

                    case stdout.getName():
                        assert putObjectRequest.getKey() ==
                            baseLocation + File.separator + pathRelativeToJobFolderParent(stdout)
                        break
                    case stderr.getName():
                        assert putObjectRequest.getKey() ==
                            baseLocation + File.separator + pathRelativeToJobFolderParent(stderr)
                        break
                    case run.getName():
                        assert putObjectRequest.getKey() ==
                            baseLocation + File.separator + pathRelativeToJobFolderParent(run)
                        break
                    case sparkDir.getName():
                        assert putObjectRequest.getKey() ==
                            baseLocation +
                            File.separator +
                            pathRelativeToJobFolderParent(sparkDir) + File.separator
                        break
                    case hadoopCoreSite.getName():
                        assert putObjectRequest.getKey() ==
                            baseLocation + File.separator + pathRelativeToJobFolderParent(hadoopCoreSite)
                        break
                    case sparkShellSetUp.getName():
                        assert putObjectRequest.getKey() ==
                            baseLocation + File.separator + pathRelativeToJobFolderParent(sparkShellSetUp)
                        break
                    case logFile.getName():
                        assert putObjectRequest.getKey() ==
                            baseLocation + File.separator + pathRelativeToJobFolderParent(logFile)
                        break
                }
        }
    }

    def "Archival Exception thrown for non S3 uri"() {

        when:
        s3ArchivalService.archive(jobDir.toPath(), new URI("file://abc"))

        then:
        thrown(ArchivalException)
    }

    def "Archival Exception thrown if there is error archiving"() {

        when:
        s3ArchivalService.archive(jobDir.toPath(), archivalLocationS3URI.getURI())

        then:
        1 * s3ClientFactory.getClient(archivalLocationS3URI) >> amazonS3
        1 * amazonS3.putObject(_ as PutObjectRequest) >> {
            throw new AmazonServiceException("test")
        }

        thrown(ArchivalException)
    }

    private String pathRelativeToJobFolderParent(File file) {
        Path jobDirPath = Paths.get(jobDir.getAbsolutePath())
        Path filePath = Paths.get(file.getAbsolutePath())

        return jobDirPath.getParent().relativize(filePath)
    }
}

