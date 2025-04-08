package com.netflix.genie.common.internal.aws.s3;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A factory class for creating and managing {@link S3TransferManager} instances.
 * This factory is responsible for creating {@link S3AsyncClient} instances and using them
 * to build {@link S3TransferManager} instances for S3 operations.
 */
@Slf4j
public class S3TransferManagerFactory {

    private final S3ClientFactory s3ClientFactory;
    private final ConcurrentHashMap<S3ClientFactory.S3ClientKey, S3AsyncClient> asyncClientCache;
    private final ConcurrentHashMap<S3AsyncClient, S3TransferManager> transferManagerCache;

    /**
     * Constructor.
     *
     * @param s3ClientFactory The S3 client factory to use for configuration and utilities
     */
    public S3TransferManagerFactory(final S3ClientFactory s3ClientFactory) {
        this.s3ClientFactory = s3ClientFactory;

        // Initialize caches
        final int initialCapacity = s3ClientFactory.getBucketProperties().size() + 1;
        this.asyncClientCache = new ConcurrentHashMap<>(initialCapacity);
        this.transferManagerCache = new ConcurrentHashMap<>(initialCapacity);
    }

    /**
     * Get an {@link S3AsyncClient} client instance appropriate for the given {@link S3Uri}.
     *
     * @param s3Uri The URI of the S3 resource this client is expected to access.
     * @return A S3 async client instance which should be used to access the S3 resource
     */
    public S3AsyncClient getAsyncClient(final S3Uri s3Uri) {
        final S3ClientFactory.S3ClientKey s3ClientKey = this.s3ClientFactory.getS3ClientKey(s3Uri);
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

    /**
     * Get a {@link S3Uri} from a URI string.
     *
     * @param uri The URI to parse
     * @return A {@link S3Uri} instance
     */
    public S3Uri getS3Uri(final URI uri) {
        return this.s3ClientFactory.getS3Uri(uri);
    }

    /**
     * Get a {@link S3Uri} from a string.
     *
     * @param uri The URI string to parse
     * @return A {@link S3Uri} instance
     */
    public S3Uri getS3Uri(final String uri) {
        return this.s3ClientFactory.getS3Uri(uri);
    }

    private S3AsyncClient buildS3AsyncClient(final S3ClientFactory.S3ClientKey s3ClientKey) {
        final AwsCredentialsProvider credentialsProvider = s3ClientKey
            .getRoleARN()
            .map(
                roleARN -> {
                    final String roleSession = "Genie-Agent-" + UUID.randomUUID().toString();

                    return (AwsCredentialsProvider) StsAssumeRoleCredentialsProvider.builder()
                        .stsClient(this.s3ClientFactory.getStsClient())
                        .refreshRequest(
                            request -> request.roleArn(roleARN).roleSessionName(roleSession)
                        )
                        .build();
                }
            )
            .orElse(this.s3ClientFactory.getAwsCredentialsProvider());

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
