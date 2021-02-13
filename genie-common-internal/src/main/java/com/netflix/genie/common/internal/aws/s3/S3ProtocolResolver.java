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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final Pair<Integer, Integer> NULL_RANGE = ImmutablePair.of(null, null);
    private static final Pattern RANGE_HEADER_PATTERN = Pattern.compile("bytes=(\\d*)-(\\d*)");

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
     * TODO: It would be nice to use Spring's HttpRange for this parsing, but this module does not
     * currently depend on spring-web. And this class cannot be moved to genie-web since it is used by
     * {@link S3ProtocolResolver} which is shared with the genie-agent module.
     */
    static Pair<Integer, Integer> parseRangeHeader(@Nullable final String rangeHeader) {
        if (StringUtils.isBlank(rangeHeader)) {
            return NULL_RANGE;
        }

        final Matcher matcher = RANGE_HEADER_PATTERN.matcher(rangeHeader);

        if (!matcher.matches()) {
            return NULL_RANGE;
        }

        final String rangeStartString = matcher.group(1);
        final String rangeEndString = matcher.group(2);

        Integer rangeStart = null;
        Integer rangeEnd = null;

        if (!StringUtils.isBlank(rangeStartString)) {
            rangeStart = Integer.parseInt(rangeStartString);
        }

        if (!StringUtils.isBlank(rangeEndString)) {
            rangeEnd = Integer.parseInt(rangeEndString);
        }

        return ImmutablePair.of(rangeStart, rangeEnd);
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
        final URI uri;
        try {
            s3URI = new AmazonS3URI(normalizedLocation);
            uri = URI.create(location);
        } catch (final IllegalArgumentException iae) {
            log.debug("{} is not a valid S3 resource (Error message: {}).", normalizedLocation, iae.getMessage());
            return null;
        }

        // Remove the fragment portion of the URI path (which stores the range requested, if any)
        final int fragmentIndex = s3URI.getKey().lastIndexOf("#");
        final String normalizedKey;
        if (fragmentIndex == -1) {
            normalizedKey = s3URI.getKey();
        } else {
            normalizedKey = s3URI.getKey().substring(0, fragmentIndex);
        }

        final String rangeHeader = uri.getFragment();
        final Pair<Integer, Integer> range = parseRangeHeader(rangeHeader);

        final AmazonS3 client = this.s3ClientFactory.getClient(s3URI);
        log.debug("{} is a valid S3 resource.", location);

        // TODO: This implementation from Spring Cloud AWS always wraps the passed in client with a proxy that follows
        //       redirects. I'm not sure if we want that or not. Probably ok for now but maybe revisit later?
        try {
            return new SimpleStorageRangeResource(
                client,
                s3URI.getBucket(),
                normalizedKey,
                s3URI.getVersionId(),
                this.s3TaskExecutor,
                range
            );
        } catch (IOException e) {
            log.error("Failed to create S3 resource: " + location + ": " + e.getMessage());
            return null;
        }
    }
}
