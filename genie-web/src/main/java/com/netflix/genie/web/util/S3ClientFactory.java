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
package com.netflix.genie.web.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.UUID;

/**
 * A class which will provide an Amazon S3 client based on configuration of the system.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class S3ClientFactory {
    private final AWSCredentialsProvider awsCredentialsProvider;
    private final ClientConfiguration awsClientConfiguration;
    private final String awsRegion;
    private final boolean assumeRole;
    private final String roleArn;
    private final AmazonS3 defaultS3Client;

    /**
     * Constructor.
     *
     * @param awsCredentialsProvider The default credentials provider for this instance
     * @param awsClientConfiguration The AWS client configuration for Genie
     * @param awsRegion              The AWS region the app is running in
     * @param roleArn                The role to assume before accessing S3 resources if necessary
     */
    public S3ClientFactory(
        @NotNull final AWSCredentialsProvider awsCredentialsProvider,
        @NotNull final ClientConfiguration awsClientConfiguration,
        @NotNull final String awsRegion,
        @Nullable final String roleArn
    ) {
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.awsClientConfiguration = awsClientConfiguration;
        this.awsRegion = awsRegion;

        this.roleArn = roleArn;
        this.assumeRole = StringUtils.isNotBlank(this.roleArn);

        // Create the default S3 client given the credentials provider and the client configuration
        this.defaultS3Client = AmazonS3ClientBuilder
            .standard()
            .withCredentials(awsCredentialsProvider)
            .withClientConfiguration(awsClientConfiguration)
            .withRegion(this.awsRegion)
            .build();
    }

    /**
     * Get an S3 client given the configuration of the system.
     *
     * @return an S3 client
     */
    public AmazonS3 getS3Client() {
        if (this.assumeRole) {
            // TODO: It's possible this could be optimized to reuse a client that a role has already been assumed for
            //       it would take more logic in this class and likely isn't worth it right now before we decide how
            //       4.x may work best. As it is now create a new client every time one is requested to assume a role

            // See: https://docs.aws.amazon.com/AmazonS3/latest/dev/AuthUsingTempSessionTokenJava.html
            final AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder
                .standard()
                .withCredentials(this.awsCredentialsProvider)
                .withClientConfiguration(this.awsClientConfiguration)
                .withRegion(this.awsRegion)
                .build();

            final AssumeRoleRequest roleRequest = new AssumeRoleRequest()
                .withRoleArn(this.roleArn)
                .withRoleSessionName("Genie-" + UUID.randomUUID().toString());

            final AssumeRoleResult roleResult = stsClient.assumeRole(roleRequest);
            final Credentials sessionCredentials = roleResult.getCredentials();

            final BasicSessionCredentials basicSessionCredentials = new BasicSessionCredentials(
                sessionCredentials.getAccessKeyId(),
                sessionCredentials.getSecretAccessKey(),
                sessionCredentials.getSessionToken()
            );

            return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(basicSessionCredentials))
                .withClientConfiguration(this.awsClientConfiguration)
                .withRegion(this.awsRegion)
                .build();
        } else {
            return this.defaultS3Client;
        }
    }
}
