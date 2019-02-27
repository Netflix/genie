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

package com.netflix.genie.web.resources.agent;

import com.netflix.genie.common.internal.dto.JobDirectoryManifest;
import com.netflix.genie.common.internal.exceptions.StreamUnavailableException;
import com.netflix.genie.web.services.AgentFileManifestService;
import com.netflix.genie.web.services.AgentFileStreamService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Resource resolver for files local to an agent running a job that can be streamed to the server and served via API.
 * The URI for such resources is: agent://\<jobId\>/\<relativePath\>.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentFileProtocolResolver implements ProtocolResolver {
    /**
     * The 'protocol' part of the URI used to identify file streamed from a live agent.
     */
    public static final String URI_SCHEME = "agent";
    private static final Path ROOT_PATH = Paths.get(".").toAbsolutePath().getRoot();
    private final AgentFileManifestService agentFileManifestService;
    private final AgentFileStreamService agentFileStreamService;

    /**
     * Constructor.
     *
     * @param agentFileManifestService the file manifest service
     * @param agentFileStreamService   the file streaming service
     */
    public AgentFileProtocolResolver(
        final AgentFileManifestService agentFileManifestService,
        final AgentFileStreamService agentFileStreamService
    ) {
        this.agentFileManifestService = agentFileManifestService;
        this.agentFileStreamService = agentFileStreamService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Resource resolve(final String location, final ResourceLoader resourceLoader) {
        log.debug("Attempting to resolve if {} is an Agent file resource or not", location);
        final URI uri;
        try {
            uri = URI.create(location);
        } catch (final IllegalArgumentException | NullPointerException e) {
            log.debug("{} is not a valid Agent resource (Error message: {}).", location, e.getMessage());
            return null;
        }

        if (URI_SCHEME.equals(uri.getScheme())) {
            log.debug("{} is a valid agent resource.", location);
            final String jobId = uri.getHost();
            final Path relativePath = ROOT_PATH.relativize(Paths.get(uri.getPath())).normalize();

            final Optional<JobDirectoryManifest> manifest = agentFileManifestService.getManifest(jobId);
            if (manifest.isPresent()) {

                final Optional<JobDirectoryManifest.ManifestEntry> optionalManifestEntry =
                    manifest.get().getEntry(relativePath.toString());

                if (optionalManifestEntry.isPresent()) {
                    final JobDirectoryManifest.ManifestEntry manifestEntry = optionalManifestEntry.get();
                    try {
                        final AgentFileStreamService.ActiveStream fileStream =
                            this.agentFileStreamService.beginFileStream(
                                jobId,
                                relativePath,
                                0,
                                manifestEntry.getSize()
                            );
                        return new AgentFileResource(uri, fileStream, manifestEntry);
                    } catch (StreamUnavailableException | IOException e) {
                        log.warn(
                            "Failed to activate stream to request: {} for job: {}: {}",
                            relativePath,
                            jobId,
                            e.getMessage(),
                            e
                        );
                    }
                } else {
                    //TODO should we return a placeholder resource with exists == false, rather than null?
                    log.warn("Could not find entry: {} in manifest of job: {}", relativePath, jobId);
                }

            } else {
                log.warn("Could not retrieve manifest for job: {}", jobId);
            }
        }

        return null;
    }
}
