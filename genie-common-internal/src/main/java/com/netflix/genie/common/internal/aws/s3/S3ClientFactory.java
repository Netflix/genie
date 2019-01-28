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

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An {@link AmazonS3} client factory class. Given {@link AmazonS3URI} instances and the configuration of the system
 * this factory is expected to return a valid client instance for the S3 URI which can then be used to access that URI.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class S3ClientFactory {

    @VisibleForTesting
    static final String BUCKET_PROPERTIES_ROOT_KEY = "genie.aws.s3.buckets";

    private final AWSCredentialsProvider awsCredentialsProvider;
    private final Map<String, S3ClientKey> bucketToClientKey;
    private final ConcurrentHashMap<S3ClientKey, AmazonS3> clientCache;
    private final ConcurrentHashMap<AmazonS3, TransferManager> transferManagerCache;
    private final Map<String, BucketProperties> bucketProperties;
    private final AWSSecurityTokenService stsClient;
    private final Regions defaultRegion;

    /**
     * Constructor.
     *
     * @param awsCredentialsProvider The base AWS credentials provider to use for the generated S3 clients
     * @param regionProvider         How this factory should determine the default {@link Regions}
     * @param environment            The Spring application {@link Environment}
     */
    public S3ClientFactory(
        final AWSCredentialsProvider awsCredentialsProvider,
        final AwsRegionProvider regionProvider,
        final Environment environment
    ) {
        this.awsCredentialsProvider = awsCredentialsProvider;

        /*
         * Use the Spring property binder to dynamically map properties under a common root into a map of key to object.
         *
         * In this case we're trying to get bucketName -> BucketProperties
         *
         * So if there were properties like:
         * genie.aws.s3.buckets.someBucket1.roleARN = blah
         * genie.aws.s3.buckets.someBucket2.region = us-east-1
         * genie.aws.s3.buckets.someBucket2.roleARN = blah
         *
         * The result of this should be two entries in the map "bucket1" and "bucket2" mapping to property binding
         * object instances of BucketProperties with the correct property set or null if option wasn't specified.
         */
        this.bucketProperties = Binder
            .get(environment)
            .bind(
                BUCKET_PROPERTIES_ROOT_KEY,
                Bindable.mapOf(String.class, BucketProperties.class)
            )
            .orElse(Collections.emptyMap());

        // Set the initial size to the number of special cases defined in properties + 1 for the default client
        // NOTE: Should we proactively create all necessary clients or be lazy about it? For now, lazy.
        final int initialCapacity = this.bucketProperties.size() + 1;
        this.clientCache = new ConcurrentHashMap<>(initialCapacity);
        this.transferManagerCache = new ConcurrentHashMap<>(initialCapacity);

        String tmpRegion;
        try {
            tmpRegion = regionProvider.getRegion();
        } catch (final SdkClientException e) {
            tmpRegion = Regions.getCurrentRegion() != null
                ? Regions.getCurrentRegion().getName()
                : Regions.US_EAST_1.getName();
            log.warn(
                "Couldn't determine the AWS region from the provider ({}) supplied. Defaulting to {}",
                regionProvider.toString(),
                tmpRegion
            );
        }
        this.defaultRegion = Regions.fromName(tmpRegion);

        // Create a token service client to use if we ever need to assume a role
        // TODO: Perhaps this should be just set to null if the bucket properties are empty as we'll never need it?
        this.stsClient = AWSSecurityTokenServiceClientBuilder
            .standard()
            .withRegion(this.defaultRegion)
            .withCredentials(this.awsCredentialsProvider)
            .build();

        this.bucketToClientKey = new ConcurrentHashMap<>();
    }

    /**
     * Get an {@link AmazonS3} client instance appropriate for the given {@link AmazonS3URI}.
     *
     * @param s3URI The URI of the S3 resource this client is expected to access.
     * @return A S3 client instance which should be used to access the S3 resource
     */
    public AmazonS3 getClient(final AmazonS3URI s3URI) {
        final String bucketName = s3URI.getBucket();

        final S3ClientKey s3ClientKey;

        /*
         * The purpose of the dual maps is to make sure we don't create an unnecessary number of S3 clients.
         * If we made the client cache just bucketName -> client directly we'd have no way to make know if an already
         * created instance for another bucket could be re-used for this bucket since it could be same region/role
         * combination. This way we first map the bucket name to a key of role/region and then use that key
         * to find a re-usable client for those dimensions.
         */
        s3ClientKey = this.bucketToClientKey.computeIfAbsent(
            bucketName,
            key -> {
                // We've never seen this bucket before. Calculate the key.

                /*
                 * Region Resolution rules:
                 * 1. Is it part of the S3 URI already? Use that
                 * 2. Is it part of the properties passed in by admin/user Use that
                 * 3. Fall back to whatever the default is for this process
                 */
                final Regions bucketRegion;
                final String uriBucketRegion = s3URI.getRegion();
                if (StringUtils.isNotBlank(uriBucketRegion)) {
                    bucketRegion = Regions.fromName(uriBucketRegion);
                } else {
                    final String propertyBucketRegion = this.bucketProperties.containsKey(key)
                        ? this.bucketProperties.get(key).getRegion().orElse(null)
                        : null;

                    if (StringUtils.isNotBlank(propertyBucketRegion)) {
                        bucketRegion = Regions.fromName(propertyBucketRegion);
                    } else {
                        bucketRegion = this.defaultRegion;
                    }
                }

                // Anything special in the bucket we need to reference
                final String roleARN = this.bucketProperties.containsKey(key)
                    ? this.bucketProperties.get(key).getRoleARN().orElse(null)
                    : null;

                return new S3ClientKey(bucketRegion, roleARN);
            }
        );

        return this.clientCache.computeIfAbsent(s3ClientKey, this::buildS3Client);
    }

    /**
     * Get a {@link TransferManager} instance for use with the given {@code s3URI}.
     *
     * @param s3URI The S3 URI this transfer manager will be interacting with
     * @return An instance of {@link TransferManager} backed by an appropriate S3 client for the given URI
     */
    public TransferManager getTransferManager(final AmazonS3URI s3URI) {
        return this.transferManagerCache.computeIfAbsent(this.getClient(s3URI), this::buildTransferManager);
    }

    private AmazonS3 buildS3Client(final S3ClientKey s3ClientKey) {
        // TODO: Do something about allowing ClientConfiguration to be passed in
        return AmazonS3ClientBuilder
            .standard()
            .withRegion(s3ClientKey.getRegion())
            .withForceGlobalBucketAccessEnabled(true)
            .withCredentials(
                s3ClientKey
                    .getRoleARN()
                    .map(
                        roleARN -> {
                            // TODO: Perhaps rename with more detailed info?
                            final String roleSession = "Genie-Agent-" + UUID.randomUUID().toString();

                            return (AWSCredentialsProvider) new STSAssumeRoleSessionCredentialsProvider
                                .Builder(roleARN, roleSession)
                                .withStsClient(this.stsClient)
                                .build();
                        }
                    )
                    .orElse(this.awsCredentialsProvider)
            )
            .build();
    }

    private TransferManager buildTransferManager(final AmazonS3 s3Client) {
        // TODO: Perhaps want to supply more options?
        return TransferManagerBuilder.standard().withS3Client(s3Client).build();
    }

    /**
     * A simple class used as a key to see if we already have a S3Client created for the combination of properties
     * that make up this class.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Getter
    @EqualsAndHashCode(doNotUseGetters = true)
    private static class S3ClientKey {
        private final Regions region;
        private final String roleARN;

        /**
         * Constructor.
         *
         * @param region  The region the S3 client is configured to access.
         * @param roleARN The role the S3 client is configured to assume if any. Null if no assumption is necessary.
         */
        S3ClientKey(final Regions region, @Nullable final String roleARN) {
            this.region = region;
            this.roleARN = roleARN;
        }

        Optional<String> getRoleARN() {
            return Optional.ofNullable(this.roleARN);
        }
    }
}
