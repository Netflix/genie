/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.configs.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.netflix.genie.web.properties.S3FileTransferProperties;
import com.netflix.genie.web.services.impl.S3FileTransferImpl;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;

/**
 * Beans and configuration specifically for S3 connection on AWS.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@Slf4j
public class AwsS3Config {

    /**
     * A bean providing a client to work with S3.
     *
     * @param noOfS3Retries          No. of S3 request retries
     * @param awsCredentialsProvider A credentials provider used to instantiate the client.
     * @return An amazon s3 client object
     */
    @Bean
    @Primary
    @ConditionalOnBean(AWSCredentialsProvider.class)
    public AmazonS3 genieS3Client(
        @Value("${genie.retry.s3.noOfRetries:5}") final int noOfS3Retries,
        final AWSCredentialsProvider awsCredentialsProvider
    ) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration()
            .withRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(noOfS3Retries));
        return AmazonS3ClientBuilder
            .standard()
            .withCredentials(awsCredentialsProvider)
            .withClientConfiguration(clientConfiguration)
            .build();
    }

    /**
     * Returns a bean which has an s3 implementation of the File Transfer interface.
     *
     * @param s3Client                 S3 client to initialize the service
     * @param registry                 The metrics registry to use
     * @param s3FileTransferProperties Configuration properties
     * @return An s3 implementation of the FileTransfer interface
     */
    @Bean(name = {"file.system.s3", "file.system.s3n", "file.system.s3a"})
    @Order(value = 1)
    @ConditionalOnBean(AmazonS3.class)
    public S3FileTransferImpl s3FileTransferImpl(
        final AmazonS3 s3Client,
        final MeterRegistry registry,
        final S3FileTransferProperties s3FileTransferProperties
    ) {
        return new S3FileTransferImpl(s3Client, registry, s3FileTransferProperties);
    }
}
