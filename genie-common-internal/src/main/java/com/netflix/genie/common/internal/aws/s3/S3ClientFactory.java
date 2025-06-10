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

import com.google.common.annotations.VisibleForTesting;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import jakarta.annotation.Nullable;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An {@link S3Client} client factory class. Given {@link S3Uri} instances and the configuration of the system
 * this factory is expected to return a valid client instance for the S3 URI which can then be used to access that URI.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class S3ClientFactory {

    @VisibleForTesting
    static final String BUCKET_PROPERTIES_ROOT_KEY = "genie.aws.s3.buckets";

    /**
     *  Get the AWS credentials provider used by this factory.
     */
    @Getter
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final Map<String, S3ClientKey> bucketToClientKey;
    private final ConcurrentHashMap<S3ClientKey, S3Client> clientCache;
    private final ConcurrentHashMap<S3ClientFactory.S3ClientKey, S3AsyncClient> asyncClientCache;
    private final ConcurrentHashMap<S3AsyncClient, S3TransferManager> transferManagerCache;
    /**
     *  Get the bucket properties used by this factory.
     */
    @Getter
    private final Map<String, BucketProperties> bucketProperties;
    /**
     *  Get the STS client used by this factory.
     */
    @Getter
    private final StsClient stsClient;
    /**
     *  Get the default region used by this factory.
     */
    @Getter
    private final Region defaultRegion;
    private final S3Utilities s3Utils;

    /**
     * Constructor.
     *
     * @param awsCredentialsProvider The base AWS credentials provider to use for the generated S3 clients
     * @param regionProvider         How this factory should determine the default {@link Region}
     * @param environment            The Spring application {@link Environment}
     */
    public S3ClientFactory(
        final AwsCredentialsProvider awsCredentialsProvider,
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

        Region tmpRegion;
        try {
            tmpRegion = regionProvider.getRegion();
        } catch (final SdkClientException e) {
            try {
                tmpRegion = new DefaultAwsRegionProviderChain().getRegion();
            } catch (final SdkClientException e2) {
                tmpRegion = Region.US_EAST_1;
            }
            log.warn(
                "Couldn't determine the AWS region from the provider ({}) supplied. Defaulting to {}",
                regionProvider.toString(),
                tmpRegion
            );
        }
        this.defaultRegion = tmpRegion;

        this.s3Utils = S3Utilities.builder().region(this.defaultRegion).build();

        // Create a token service client to use if we ever need to assume a role
        // TODO: Perhaps this should be just set to null if the bucket properties are empty as we'll never need it?
        this.stsClient = StsClient.builder()
            .region(this.defaultRegion)
            .credentialsProvider(this.awsCredentialsProvider)
            .build();

        this.bucketToClientKey = new ConcurrentHashMap<>();
        this.asyncClientCache = new ConcurrentHashMap<>(initialCapacity);
        this.transferManagerCache = new ConcurrentHashMap<>(initialCapacity);
    }

    /**
     * Get an {@link S3Client} client instance appropriate for the given {@link S3Uri}.
     *
     * @param s3Uri The URI of the S3 resource this client is expected to access.
     * @return A S3 client instance which should be used to access the S3 resource
     */
    public S3Client getClient(final S3Uri s3Uri) {
        final S3ClientKey s3ClientKey = getS3ClientKey(s3Uri);
        return this.clientCache.computeIfAbsent(s3ClientKey, this::buildS3Client);
    }

    /**
     * Get a {@link S3Uri} from a URI.
     *
     * @param uri The URI to parse
     * @return A {@link S3Uri} instance
     */
    public S3Uri getS3Uri(final URI uri) {
        return this.s3Utils.parseUri(uri);
    }

    /**
     * Get a {@link S3Uri} from a string.
     *
     * @param uri The URI string to parse
     * @return A {@link S3Uri} instance
     */
    public S3Uri getS3Uri(final String uri) {
        return this.getS3Uri(URI.create(uri));
    }

    /**
     * Get the S3 client key for a given S3 URI.
     *
     * @param s3Uri The S3 URI
     * @return The S3 client key
     */
    public S3ClientKey getS3ClientKey(final S3Uri s3Uri) {
        final String bucketName = s3Uri.bucket().orElse(null);

        /*
         * The purpose of the dual maps is to make sure we don't create an unnecessary number of S3 clients.
         * If we made the client cache just bucketName -> client directly we'd have no way to make know if an already
         * created instance for another bucket could be re-used for this bucket since it could be same region/role
         * combination. This way we first map the bucket name to a key of role/region and then use that key
         * to find a re-usable client for those dimensions.
         */
        return this.bucketToClientKey.computeIfAbsent(
            bucketName,
            key -> {
                // We've never seen this bucket before. Calculate the key.

                /*
                 * Region Resolution rules:
                 * 1. Is it part of the S3 URI already? Use that
                 * 2. Is it part of the properties passed in by admin/user Use that
                 * 3. Fall back to whatever the default is for this process
                 */
                final Region bucketRegion;
                final Optional<Region> regionOptional = s3Uri.region();
                if (regionOptional.isPresent()) {
                    bucketRegion = regionOptional.get();
                } else {
                    final String propertyBucketRegion = this.bucketProperties.containsKey(key)
                        ? this.bucketProperties.get(key).getRegion().orElse(null)
                        : null;

                    if (StringUtils.isNotBlank(propertyBucketRegion)) {
                        bucketRegion = Region.of(propertyBucketRegion);
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
    }

    private S3Client buildS3Client(final S3ClientKey s3ClientKey) {
        // TODO: Do something about allowing ClientConfiguration to be passed in
        return S3Client.builder()
            .region(s3ClientKey.getRegion())
            .credentialsProvider(
                s3ClientKey
                    .getRoleARN()
                    .map(
                        roleARN -> {
                            // TODO: Perhaps rename with more detailed info?
                            final String roleSession = "Genie-Agent-" + UUID.randomUUID().toString();

                            return (AwsCredentialsProvider) StsAssumeRoleCredentialsProvider.builder()
                                .stsClient(this.stsClient)
                                .refreshRequest(
                                    request -> request.roleArn(roleARN).roleSessionName(roleSession)
                                )
                                .build();
                        }
                    )
                    .orElse(this.awsCredentialsProvider)
            )
            .build();
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
    public static class S3ClientKey {
        private final Region region;
        private final String roleARN;

        /**
         * Constructor.
         *
         * @param region  The region the S3 client is configured to access.
         * @param roleARN The role the S3 client is configured to assume if any. Null if no assumption is necessary.
         */
        S3ClientKey(final Region region, @Nullable final String roleARN) {
            this.region = region;
            this.roleARN = roleARN;
        }

        /**
         * Get the role ARN.
         *
         * @return The role ARN as an Optional
         */
        public Optional<String> getRoleARN() {
            return Optional.ofNullable(this.roleARN);
        }
    }

    /**
     * Get an {@link S3AsyncClient} client instance appropriate for the given {@link S3Uri}.
     *
     * @param s3Uri The URI of the S3 resource this client is expected to access.
     * @return A S3 async client instance which should be used to access the S3 resource
     */
    public S3AsyncClient getAsyncClient(final S3Uri s3Uri) {
        final S3ClientKey s3ClientKey = getS3ClientKey(s3Uri);
        return this.asyncClientCache.computeIfAbsent(s3ClientKey, this::buildS3AsyncClient);
    }

    /**
     * Get a {@link S3TransferManager} instance for use with the given {@code s3Uri}.
     *
     * @param s3Uri The S3 URI this transfer manager will be interacting with
     * @return An instance of {@link S3TransferManager} backed by an appropriate S3 async client for the given URI
     */
    public S3TransferManager getTransferManager(final S3Uri s3Uri) {
        return this.transferManagerCache.computeIfAbsent(this.getAsyncClient(s3Uri), this::buildTransferManager);
    }

    private S3AsyncClient buildS3AsyncClient(final S3ClientKey s3ClientKey) {
        final AwsCredentialsProvider credentialsProvider = s3ClientKey
            .getRoleARN()
            .map(
                roleARN -> {
                    final String roleSession = "Genie-Agent-" + UUID.randomUUID().toString();

                    return (AwsCredentialsProvider) StsAssumeRoleCredentialsProvider.builder()
                        .stsClient(getStsClient())
                        .refreshRequest(
                            request -> request.roleArn(roleARN).roleSessionName(roleSession)
                        )
                        .build();
                }
            )
            .orElse(getAwsCredentialsProvider());

        return S3AsyncClient.builder()
            .region(s3ClientKey.getRegion())
            .credentialsProvider(credentialsProvider)
            .build();
    }

    private S3TransferManager buildTransferManager(final S3AsyncClient s3AsyncClient) {
        return S3TransferManager.builder()
            .s3Client(s3AsyncClient)
            .build();
    }
}
