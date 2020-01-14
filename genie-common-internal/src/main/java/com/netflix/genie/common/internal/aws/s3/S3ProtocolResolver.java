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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.aws.core.io.s3.SimpleStorageResource;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;

/**
 * This class implements the {@link ProtocolResolver} interface. When an instance of this class is added to a
 * Spring Application context list of Protocol Resolvers via
 * {@link org.springframework.context.ConfigurableApplicationContext#addProtocolResolver(ProtocolResolver)} allows
 * valid S3 resources to be loaded using the Spring {@link ResourceLoader} abstraction.
 * <p>
 * Leverages some work done by Spring Cloud AWS.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class S3ProtocolResolver implements ProtocolResolver {

    private static final String S3N_PROTOCOL = "s3n:";
    private static final String S3A_PROTOCOL = "s3a:";
    private static final String S3_REGEX = "s3.:";
    private static final String S3_REPLACEMENT = "s3:";

    private final S3ClientFactory s3ClientFactory;
    private final TaskExecutor s3TaskExecutor;

    /**
     * Constructor.
     *
     * @param s3ClientFactory The S3 client factory to use to get S3 client instances
     * @param s3TaskExecutor  A task executor to use for uploading files to S3
     */
    public S3ProtocolResolver(
        final S3ClientFactory s3ClientFactory,
        final TaskExecutor s3TaskExecutor
    ) {
        this.s3ClientFactory = s3ClientFactory;
        this.s3TaskExecutor = s3TaskExecutor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Resource resolve(final String location, final ResourceLoader resourceLoader) {
        log.debug("Attempting to resolve if {} is a S3 resource or not", location);

        final String normalizedLocation;
        // Rewrite s3n:// and s3a:// URIs as s3:// for backward compatibility
        if (location.startsWith(S3N_PROTOCOL) || location.startsWith(S3A_PROTOCOL)) {
            normalizedLocation = location.replaceFirst(S3_REGEX, S3_REPLACEMENT);
        } else {
            normalizedLocation = location;
        }

        final AmazonS3URI s3URI;
        try {
            s3URI = new AmazonS3URI(normalizedLocation);
        } catch (final IllegalArgumentException iae) {
            log.debug("{} is not a valid S3 resource (Error message: {}).", normalizedLocation, iae.getMessage());
            return null;
        }

        final AmazonS3 client = this.s3ClientFactory.getClient(s3URI);

        log.debug("{} is a valid S3 resource.", location);

        // TODO: This implementation from Spring Cloud AWS always wraps the passed in client with a proxy that follows
        //       redirects. I'm not sure if we want that or not. Probably ok for now but maybe revisit later?
        return new SimpleStorageResource(
            client,
            s3URI.getBucket(),
            s3URI.getKey(),
            this.s3TaskExecutor,
            s3URI.getVersionId()
        );
    }
}
