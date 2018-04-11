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
package com.netflix.genie.web.configs.aws;

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * A configuration that is conditional on whether users want to assume a role for accessing AWS resources.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@ConditionalOnProperty("genie.aws.credentials.role")
@Slf4j
public class AwsCredentialsConfig {
    /**
     * Assume role credentials provider which will be used to fetch session credentials.
     *
     * @param roleArn Arn of the IAM role
     * @return Credentials provider to ask the credentials from
     */
    @Bean
    @Primary
    public STSAssumeRoleSessionCredentialsProvider awsCredentialsProvider(
        @Value("${genie.aws.credentials.role}") final String roleArn
    ) {
        log.info("Creating STS Assume Role Session Credentials provider bean");
        return new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, "Genie").build();
    }
}
