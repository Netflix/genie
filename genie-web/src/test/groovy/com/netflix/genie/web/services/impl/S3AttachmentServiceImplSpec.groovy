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

import com.google.common.collect.Sets;
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.web.exceptions.checked.AttachmentTooLargeException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import com.netflix.genie.web.properties.AttachmentServiceProperties;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.core.io.Resource;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import spock.lang.Specification;
import spock.lang.Unroll;

import java.util.concurrent.TimeUnit;

class S3AttachmentServiceImplSpec extends Specification {
    public static final String BUCKET_NAME = "some-bucket"
    public static final String S3_PREFIX = "some/prefix"
    S3ClientFactory s3ClientFactory
    AttachmentServiceProperties serviceProperties
    MeterRegistry registry
    S3AttachmentServiceImpl service
    DistributionSummary distributionSummary
    Timer timer
    S3Client s3Client
    InputStream inputStream
    S3Uri s3Uri

    void setup() {
        this.distributionSummary = Mock(DistributionSummary)
        this.timer = Mock(Timer)
        this.s3Client = Mock(S3Client)
        this.inputStream = Mock(InputStream)
        this.s3Uri = Mock(S3Uri)

        this.s3ClientFactory = Mock(S3ClientFactory)
        this.serviceProperties = new AttachmentServiceProperties()
        this.registry = Mock(MeterRegistry)

        this.serviceProperties.setLocationPrefix(URI.create("s3://" + BUCKET_NAME + "/" + S3_PREFIX))

        s3ClientFactory.getS3Uri(serviceProperties.getLocationPrefix()) >> s3Uri
        s3Uri.bucket() >> Optional.of(BUCKET_NAME)
        s3Uri.key() >> Optional.of(S3_PREFIX)

        this.service = new S3AttachmentServiceImpl(s3ClientFactory, serviceProperties, registry)
    }

    @Unroll
    def "No attachments (job id present: #jobIdPresent)"() {
        setup:
        String jobId = jobIdPresent ? UUID.randomUUID().toString() : null

        when:
        Set<URI> attachmentURIs = this.service.saveAttachments(jobId, Sets.newHashSet())

        then:
        1 * registry.summary(S3AttachmentServiceImpl.COUNT_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(0)
        attachmentURIs.isEmpty()

        where:
        jobIdPresent << [true, false]
    }

    @Unroll
    def "Pre-upload errors (job id present: #jobIdPresent)"() {
        setup:
        String jobId = jobIdPresent ? UUID.randomUUID().toString() : null
        Resource attachment1 = Mock(Resource)
        Resource attachment2 = Mock(Resource)
        serviceProperties.setMaxSize(DataSize.ofBytes(60))
        serviceProperties.setMaxTotalSize(DataSize.ofBytes(100))

        when: "Attachment content throws IOException"
        this.service.saveAttachments(jobId, Sets.newHashSet(attachment1))

        then:
        1 * registry.summary(S3AttachmentServiceImpl.COUNT_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(1)
        1 * attachment1.getFilename() >> "script.sql"
        1 * attachment1.contentLength() >> { throw new IOException("...") }
        thrown(SaveAttachmentException)

        when: "Attachment size too large"
        this.service.saveAttachments(jobId, Sets.newHashSet(attachment1))

        then:
        1 * registry.summary(S3AttachmentServiceImpl.COUNT_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(1)
        1 * attachment1.getFilename() >> "script.sql"
        1 * attachment1.contentLength() >> 80
        thrown(AttachmentTooLargeException)

        when: "Attachments total size too large"
        this.service.saveAttachments(jobId, Sets.newHashSet(attachment1, attachment2))

        then:
        1 * registry.summary(S3AttachmentServiceImpl.COUNT_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(2)
        1 * attachment1.getFilename() >> "script1.sql"
        1 * attachment1.contentLength() >> 60
        1 * attachment2.getFilename() >> "script2.sql"
        1 * attachment2.contentLength() >> 60
        thrown(AttachmentTooLargeException)

        where:
        jobIdPresent << [true, false]
    }

    @Unroll
    def "Successful (job id present: #jobIdPresent)"() {
        setup:
        String jobId = jobIdPresent ? UUID.randomUUID().toString() : null
        Resource attachment1 = Mock(Resource)
        Resource attachment2 = Mock(Resource)
        byte[] content1 = new byte[DataSize.ofMegabytes(3).toBytes() as int]
        byte[] content2 = new byte[DataSize.ofMegabytes(5).toBytes() as int]

        when: "Attachments upload successfully"
        Set<URI> attachmentUris = this.service.saveAttachments(jobId, Sets.newHashSet(attachment1, attachment2))

        then:
        1 * registry.summary(S3AttachmentServiceImpl.COUNT_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(2)
        1 * attachment1.getFilename() >> "script1.sql"
        1 * attachment1.contentLength() >> DataSize.ofMegabytes(3).toBytes()
        1 * attachment2.getFilename() >> "script2.sql"
        1 * attachment2.contentLength() >> DataSize.ofMegabytes(5).toBytes()
        1 * registry.summary(S3AttachmentServiceImpl.LARGEST_SIZE_DISTRIBUTION) >> distributionSummary
        1 * registry.summary(S3AttachmentServiceImpl.TOTAL_SIZE_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(5 * 1024 * 1024)
        1 * distributionSummary.record((5 + 3) * 1024 * 1024)
        1 * s3ClientFactory.getClient(s3Uri) >> s3Client

        1 * attachment1.getFilename() >> "script1.sql"
        1 * attachment1.contentLength() >> DataSize.ofMegabytes(3).toBytes()
        1 * attachment1.getInputStream() >> inputStream
        1 * inputStream.readAllBytes() >> content1

        1 * attachment2.getFilename() >> "script2.sql"
        1 * attachment2.contentLength() >> DataSize.ofMegabytes(5).toBytes()
        1 * attachment2.getInputStream() >> inputStream
        1 * inputStream.readAllBytes() >> content2

        2 * inputStream.close()
        2 * s3Client.putObject(
            { PutObjectRequest request ->
                request.bucket() == BUCKET_NAME &&
                    request.key() =~ /some\/prefix\/.+\/script[12]\.sql/ &&
                    (request.contentLength() == DataSize.ofMegabytes(3).toBytes() ||
                        request.contentLength() == DataSize.ofMegabytes(5).toBytes())
            },
            _ as RequestBody
        )
        1 * registry.timer(S3AttachmentServiceImpl.SAVE_TIMER, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)

        attachmentUris.size() == 2
        attachmentUris.findAll({ it.toString() ==~ /s3:\/\/some-bucket\/some\/prefix\/.+\/script[12]\.sql/ }).size() == 2

        where:
        jobIdPresent << [true, false]
    }

    def "Verify content length mismatch handling"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        Resource attachment = Mock(Resource)
        InputStream mockStream = Mock(InputStream)
        s3ClientFactory.getClient(s3Uri) >> s3Client
        byte[] shorterContent = new byte[DataSize.ofMegabytes(9).toBytes() as int]

        when: "Upload attachment with content length mismatch"
        this.service.saveAttachments(jobId, Sets.newHashSet(attachment))

        then: "Verify debug log is used and actual size is used"
        1 * registry.summary(S3AttachmentServiceImpl.COUNT_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(1)
        2 * attachment.getFilename() >> "test.sql"
        2 * attachment.contentLength() >> DataSize.ofMegabytes(10).toBytes()
        1 * registry.summary(S3AttachmentServiceImpl.LARGEST_SIZE_DISTRIBUTION) >> distributionSummary
        1 * registry.summary(S3AttachmentServiceImpl.TOTAL_SIZE_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(10 * 1024 * 1024)
        1 * distributionSummary.record(10 * 1024 * 1024)
        1 * attachment.getInputStream() >> mockStream
        1 * mockStream.readAllBytes() >> shorterContent
        1 * mockStream.close()

        1 * s3Client.putObject(
            { PutObjectRequest request ->
                request.contentLength() == shorterContent.length
            },
            { RequestBody capturedBody ->
                capturedBody != null
            }
        )

        1 * registry.timer(S3AttachmentServiceImpl.SAVE_TIMER, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
    }

    @Unroll
    def "Upload errors (job id present: #jobIdPresent)"() {
        setup:
        String jobId = jobIdPresent ? UUID.randomUUID().toString() : null
        Resource attachment1 = Mock(Resource)
        s3ClientFactory.getClient(s3Uri) >> s3Client
        byte[] content = new byte[DataSize.ofMegabytes(3).toBytes() as int]

        when: "S3 upload fails"
        this.service.saveAttachments(jobId, Sets.newHashSet(attachment1))

        then:
        1 * registry.summary(S3AttachmentServiceImpl.COUNT_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(1)
        1 * attachment1.getFilename() >> "script.sql"
        1 * attachment1.contentLength() >> DataSize.ofMegabytes(3).toBytes()
        1 * registry.summary(S3AttachmentServiceImpl.LARGEST_SIZE_DISTRIBUTION) >> distributionSummary
        1 * registry.summary(S3AttachmentServiceImpl.TOTAL_SIZE_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(3 * 1024 * 1024)
        1 * distributionSummary.record(3 * 1024 * 1024)
        1 * attachment1.getFilename() >> "script.sql"
        1 * attachment1.contentLength() >> DataSize.ofMegabytes(3).toBytes()
        1 * attachment1.getInputStream() >> inputStream
        1 * inputStream.readAllBytes() >> content
        1 * inputStream.close()
        1 * s3Client.putObject(
            { PutObjectRequest request ->
                request.bucket() == BUCKET_NAME &&
                    request.key() =~ /some\/prefix\/.+\/script\.sql/ &&
                    request.contentLength() == content.length
            },
            _ as RequestBody
        ) >> {
            throw SdkClientException.builder().message("...").build()
        }
        1 * registry.timer(S3AttachmentServiceImpl.SAVE_TIMER, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        thrown(SaveAttachmentException)

        where:
        jobIdPresent << [true, false]
    }

    @Unroll
    def "Invalid attachment (filename: #attachmentFilename)"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        Resource attachment1 = Mock(Resource)
        s3ClientFactory.getClient(s3Uri) >> s3Client

        when: "Attachment has invalid filename"
        this.service.saveAttachments(jobId, Sets.newHashSet(attachment1))

        then:
        1 * registry.summary(S3AttachmentServiceImpl.COUNT_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(1)
        1 * attachment1.getFilename() >> attachmentFilename
        1 * attachment1.contentLength() >> DataSize.ofMegabytes(3).toBytes()
        1 * registry.summary(S3AttachmentServiceImpl.LARGEST_SIZE_DISTRIBUTION) >> distributionSummary
        1 * registry.summary(S3AttachmentServiceImpl.TOTAL_SIZE_DISTRIBUTION) >> distributionSummary
        1 * distributionSummary.record(3 * 1024 * 1024)
        1 * distributionSummary.record(3 * 1024 * 1024)
        1 * attachment1.getFilename() >> attachmentFilename
        0 * attachment1.getInputStream()
        0 * s3Client.putObject(*_)
        1 * registry.timer(S3AttachmentServiceImpl.SAVE_TIMER, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        thrown(SaveAttachmentException)

        where:
        attachmentFilename << [null, "", " "]
    }
}
