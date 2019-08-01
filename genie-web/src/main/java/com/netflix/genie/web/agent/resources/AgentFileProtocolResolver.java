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
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Resource resolver for files local to an agent running a job that can be streamed to the server and served via API.
 * The URI for such resources is: agent://\<jobId\>/\<relativePath\>.
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
     * @param jobId the job id
     * @param path  the path of the file within the job directory
     * @return a {@link URI} representing the remote agent file
     * @throws URISyntaxException if constructing the URI fails
     */
    public static URI createUri(final String jobId, final String path) throws URISyntaxException {
        return new URI(AgentFileProtocolResolver.URI_SCHEME, jobId, path, null);
    }

    private static String getAgentResourceURIFileJobId(final URI agentUri) {
        if (!URI_SCHEME.equals(agentUri.getScheme())) {
            throw new IllegalArgumentException("Not a valid Agent resource URI: " + agentUri);
        }
        return agentUri.getHost();
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

        final AgentFileStreamService.AgentFileResource resourceOrNull =
            agentFileStreamService.getResource(jobId, relativePath, uri).orElse(null);

        if (resourceOrNull != null) {
            log.debug("Returning resource: {}", resourceOrNull.getDescription());
        }
        return resourceOrNull;
    }
}
