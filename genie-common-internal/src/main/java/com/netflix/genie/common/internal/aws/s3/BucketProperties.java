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
import org.springframework.validation.annotation.Validated;

import jakarta.annotation.Nullable;
import java.util.Optional;

/**
 * A property class which holds information about how to interact with a specific S3 Bucket.
 */
@Validated
@Getter
@Setter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
public class BucketProperties {

    private static final String IAM_SERVICE_NAMESPACE = "iam";

    private String roleARN;
    private Regions region;

    public Optional<String> getRegion() {
        if (this.region == null) {
            return Optional.empty();
        } else {
            return Optional.of(this.region.getName());
        }
    }

    public void setRegion(@Nullable final String region) {
        if (region != null) {
            this.region = Regions.fromName(region);
        } else {
            this.region = null;
        }
    }

    public Optional<String> getRoleARN() {
        if (this.roleARN == null) {
            return Optional.empty();
        } else {
            return Optional.of(this.roleARN);
        }
    }

    public void setRoleARN(@Nullable final String roleARN) {
        if (roleARN != null) {
            if (isValidRoleARN(roleARN)) {
                this.roleARN = roleARN;
            } else {
                throw new IllegalArgumentException(
                    "ARN (" + roleARN + ") is valid format but incorrect service. Expected "
                        + IAM_SERVICE_NAMESPACE + " but got " + getServiceFromARN(roleARN)
                );
            }
        }
    }

    private boolean isValidRoleARN(String arn) {
        // Basic validation for ARN format and service
        String[] arnParts = arn.split(":");
        return arnParts.length > 2 && IAM_SERVICE_NAMESPACE.equals(getServiceFromARN(arn));
    }

    private String getServiceFromARN(String arn) {
        // Extract the service part from the ARN
        String[] arnParts = arn.split(":");
        return arnParts.length > 2 ? arnParts[2] : "";
    }
}
