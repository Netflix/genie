/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.agent.resources;

import com.netflix.genie.web.agent.services.AgentFileStreamService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.utils.URIBuilder;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpRange;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Resource resolver for files local to an agent running a job that can be streamed to the server and served via API.
 * The URI for such resources is: {@literal agent://<jobId>/<relativePath>}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentFileProtocolResolver implements ProtocolResolver {

    private static final String URI_SCHEME = "agent";
    private static final Path ROOT_PATH = Paths.get(".").toAbsolutePath().getRoot();

    private final AgentFileStreamService agentFileStreamService;

    /**
     * Constructor.
     *
     * @param agentFileStreamService the agent file stream service
     */
    public AgentFileProtocolResolver(
        final AgentFileStreamService agentFileStreamService
    ) {
        this.agentFileStreamService = agentFileStreamService;
    }

    /**
     * Create a URI for the given remote agent file.
     *
     * @param jobId       the job id
     * @param path        the path of the file within the job directory
     * @param rangeHeader the request range header (as per RFC 7233)
     * @return a {@link URI} representing the remote agent file
     * @throws URISyntaxException if constructing the URI fails
     */
    public static URI createUri(
        final String jobId,
        final String path,
        @Nullable final String rangeHeader
    ) throws URISyntaxException {
        final String encodedJobId = Base64.encodeBase64URLSafeString(jobId.getBytes(Charset.defaultCharset()));
        return new URIBuilder()
            .setScheme(URI_SCHEME)
            .setHost(encodedJobId)
            .setPath(path)
            .setFragment(rangeHeader)
            .build();
    }

    private static String getAgentResourceURIFileJobId(final URI agentUri) {
        if (!URI_SCHEME.equals(agentUri.getScheme())) {
            throw new IllegalArgumentException("Not a valid Agent resource URI: " + agentUri);
        }
        return new String(Base64.decodeBase64(agentUri.getHost()), Charset.defaultCharset());
    }

    private static String getAgentResourceURIFilePath(final URI agentUri) {
        if (!URI_SCHEME.equals(agentUri.getScheme())) {
            throw new IllegalArgumentException("Not a valid Agent resource URI: " + agentUri);
        }
        return agentUri.getPath();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Resource resolve(final String location, final ResourceLoader resourceLoader) {
        log.debug("Attempting to resolve if {} is an Agent file resource or not", location);
        final URI uri;
        final String jobId;
        final Path relativePath;

        try {
            uri = URI.create(location);
            jobId = getAgentResourceURIFileJobId(uri);
            relativePath = ROOT_PATH.relativize(Paths.get(getAgentResourceURIFilePath(uri))).normalize();
        } catch (final IllegalArgumentException | NullPointerException e) {
            log.debug("{} is not a valid Agent resource (Error message: {}).", location, e.getMessage());
            return null;
        }

        final String rangeHeader = uri.getFragment();

        final List<HttpRange> ranges;
        try {
            ranges = HttpRange.parseRanges(rangeHeader);
        } catch (final IllegalArgumentException | NullPointerException e) {
            log.warn("Invalid range header '{}' (Error message: {}).", rangeHeader, e.getMessage());
            return null;
        }

        if (ranges.size() > 1) {
            log.warn("Multiple HTTP ranges not supported");
            return null;
        }

        final HttpRange rangeOrNull = ranges.isEmpty() ? null : ranges.get(0);

        final AgentFileStreamService.AgentFileResource resourceOrNull =
            agentFileStreamService.getResource(jobId, relativePath, uri, rangeOrNull).orElse(null);

        if (resourceOrNull != null) {
            log.debug("Returning resource: {}", resourceOrNull.getDescription());
        }

        return resourceOrNull;
    }
}
