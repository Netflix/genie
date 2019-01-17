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
package com.netflix.genie.common.internal.aws.s3;

import com.amazonaws.regions.Regions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.cloud.aws.core.naming.AmazonResourceName;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * A property class which holds information about how to interact with a specific S3 Bucket.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
@Getter
@Setter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
public class BucketProperties {

    /*
     * See: https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#genref-aws-service-namespaces
     */
    private static final String IAM_SERVICE_NAMESPACE = "iam";

    private AmazonResourceName roleARN;
    private Regions region;

    /**
     * Get the {@link Regions} this bucket is in.
     *
     * @return The {@link Regions#getName()} wrapped in an {@link Optional}. If the optional is empty it indicates that
     * the default or current region should be used
     */
    public Optional<String> getRegion() {
        if (this.region == null) {
            return Optional.empty();
        } else {
            return Optional.of(this.region.getName());
        }
    }

    /**
     * Set the AWS region from a string name representation e.g. us-east-1.
     *
     * @param region The name of the region to use
     * @see Regions#fromName(String)
     */
    public void setRegion(@Nullable final String region) {
        if (region != null) {
            this.region = Regions.fromName(region);
        } else {
            this.region = null;
        }
    }

    /**
     * Get the ARN of the role to assume from this instance when working with the given bucket.
     *
     * @return The ARN wrapped in an {@link Optional}. If the {@link Optional} is empty no role should be assumed when
     * working with this bucket
     */
    public Optional<String> getRoleARN() {
        if (this.roleARN == null) {
            return Optional.empty();
        } else {
            return Optional.of(this.roleARN.toString());
        }
    }

    /**
     * Set the ARN of the role to assume from this instance when working with the given bucket.
     *
     * @param roleARN The valid role ARN or null if no role assumption is needed.
     * @throws IllegalArgumentException If the {@code roleARN} is not null and the value isn't a valid role ARN format
     */
    public void setRoleARN(@Nullable final String roleARN) {
        if (roleARN != null) {
            final AmazonResourceName arn = AmazonResourceName.fromString(roleARN);
            final String awsService = arn.getService();
            if (awsService.equals(IAM_SERVICE_NAMESPACE)) {
                this.roleARN = arn;
            } else {
                throw new IllegalArgumentException(
                    "ARN ("
                        + roleARN
                        + ") is valid format but incorrect service. Expected "
                        + IAM_SERVICE_NAMESPACE
                        + " but got "
                        + awsService
                );
            }
        }
    }
}
