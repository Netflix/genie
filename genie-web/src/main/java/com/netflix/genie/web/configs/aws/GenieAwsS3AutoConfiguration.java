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
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.netflix.genie.web.properties.AwsCredentialsProperties;
import com.netflix.genie.web.properties.RetryProperties;
import com.netflix.genie.web.properties.S3FileTransferProperties;
import com.netflix.genie.web.services.impl.S3FileTransferImpl;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.aws.autoconfigure.context.ContextCredentialsAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.util.UUID;

/**
 * Beans and configuration specifically for S3 connection on AWS.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@ConditionalOnBean(AWSCredentialsProvider.class)
@AutoConfigureAfter(ContextCredentialsAutoConfiguration.class)
@EnableConfigurationProperties(
    {
        AwsCredentialsProperties.class,
        AwsCredentialsProperties.SpringCloudAwsRegionProperties.class,
        RetryProperties.class,
        S3FileTransferProperties.class
    }
)
@Slf4j
public class GenieAwsS3AutoConfiguration {

    /**
     * Default AWS client configuration that sets the number of retries provided by the user.
     *
     * @param retryProperties The properties related to retry configuration in Genie
     * @return The client configuration to use for all built AWS clients
     */
    @Bean
    @ConditionalOnMissingBean(name = "genieAwsClientConfiguration", value = ClientConfiguration.class)
    public ClientConfiguration genieAwsClientConfiguration(final RetryProperties retryProperties) {
        return new ClientConfiguration().withRetryPolicy(
            PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(retryProperties.getS3().getNoOfRetries())
        );
    }

    /**
     * Amazon S3 client configured using either the default credentials provider or an
     * {@link STSAssumeRoleSessionCredentialsProvider} if a role is desired to be assumed.
     *
     * @param credentialsProvider            The default credentials provider to use
     * @param clientConfiguration            The client configuration to use
     * @param springCloudAwsRegionProperties Properties for the region coming from Spring Cloud AWS
     * @param awsCredentialsProperties       The properties under the Genie namespace related to AWS context
     * @return An amazon s3 client object
     */
    @Bean
    public AmazonS3 amazonS3(
        final AWSCredentialsProvider credentialsProvider,
        final ClientConfiguration clientConfiguration,
        final AwsCredentialsProperties.SpringCloudAwsRegionProperties springCloudAwsRegionProperties,
        final AwsCredentialsProperties awsCredentialsProperties
    ) {
        final String roleArn = awsCredentialsProperties.getRole();
        final Regions region = springCloudAwsRegionProperties.getRegion();
        final boolean assumeRole = StringUtils.isNotBlank(roleArn);
        final AWSCredentialsProvider s3CredentialsProvider;
        if (assumeRole) {
            log.info("AWS configured to assume role {}", roleArn);

            // See: https://docs.aws.amazon.com/AmazonS3/latest/dev/AuthUsingTempSessionTokenJava.html
            final AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withClientConfiguration(clientConfiguration)
                .withRegion(region)
                .build();

            final String roleSession = "Genie-" + UUID.randomUUID().toString();
            log.info("Role session name for this instance is {}", roleSession);

            s3CredentialsProvider = new STSAssumeRoleSessionCredentialsProvider
                .Builder(roleArn, roleSession)
                .withStsClient(stsClient)
                .build();
        } else {
            s3CredentialsProvider = credentialsProvider;
        }

        return AmazonS3ClientBuilder
            .standard()
            .withCredentials(s3CredentialsProvider)
            .withClientConfiguration(clientConfiguration)
            .withRegion(region)
            .build();
    }

    /**
     * Returns a bean which has an s3 implementation of the File Transfer interface.
     *
     * @param amazonS3                 S3 client to use
     * @param registry                 The metrics registry to use
     * @param s3FileTransferProperties Configuration properties
     * @return An s3 implementation of the FileTransfer interface
     */
    @Bean(name = {"file.system.s3", "file.system.s3n", "file.system.s3a"})
    @Order(value = 1)
    public S3FileTransferImpl s3FileTransferImpl(
        final AmazonS3 amazonS3,
        final MeterRegistry registry,
        final S3FileTransferProperties s3FileTransferProperties
    ) {
        return new S3FileTransferImpl(amazonS3, registry, s3FileTransferProperties);
    }
}
