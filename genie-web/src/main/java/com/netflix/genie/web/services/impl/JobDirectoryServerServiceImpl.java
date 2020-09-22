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
package com.netflix.genie.web.services.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.dtos.DirectoryManifest;
import com.netflix.genie.web.agent.resources.AgentFileProtocolResolver;
import com.netflix.genie.web.agent.services.AgentFileStreamService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.dtos.ArchivedJobMetadata;
import com.netflix.genie.web.exceptions.checked.JobDirectoryManifestNotFoundException;
import com.netflix.genie.web.exceptions.checked.JobNotArchivedException;
import com.netflix.genie.web.exceptions.checked.JobNotFoundException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.resources.writers.DefaultDirectoryWriter;
import com.netflix.genie.web.services.ArchivedJobService;
import com.netflix.genie.web.services.JobDirectoryServerService;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.utils.URIBuilder;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.ResourceRegionHttpMessageConverter;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@link JobDirectoryServerService}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class JobDirectoryServerServiceImpl implements JobDirectoryServerService {

    private static final String SLASH = "/";
    private static final String SERVE_RESOURCE_TIMER = "genie.files.serve.timer";
    private static final String ARCHIVE_STATUS_TAG = "archiveStatus";

    private final ResourceLoader resourceLoader;
    private final PersistenceService persistenceService;
    private final AgentFileStreamService agentFileStreamService;
    private final MeterRegistry meterRegistry;
    private final GenieResourceHandler.Factory genieResourceHandlerFactory;
    private final ArchivedJobService archivedJobService;
    private final AgentRoutingService agentRoutingService;

    /**
     * Constructor.
     *
     * @param resourceLoader         The application resource loader used to get references to resources
     * @param dataServices           The {@link DataServices} instance to use
     * @param agentFileStreamService The service providing file manifest for active agent jobs
     * @param archivedJobService     The {@link ArchivedJobService} implementation to use to get archived
     *                               job data
     * @param meterRegistry          The meter registry used to keep track of metrics
     * @param agentRoutingService    The agent routing service
     */
    public JobDirectoryServerServiceImpl(
        final ResourceLoader resourceLoader,
        final DataServices dataServices,
        final AgentFileStreamService agentFileStreamService,
        final ArchivedJobService archivedJobService,
        final MeterRegistry meterRegistry,
        final AgentRoutingService agentRoutingService
    ) {
        this(
            resourceLoader,
            dataServices,
            agentFileStreamService,
            archivedJobService,
            new GenieResourceHandler.Factory(),
            meterRegistry,
            agentRoutingService
        );
    }

    /**
     * Constructor that accepts a handler factory mock for easier testing.
     */
    @VisibleForTesting
    JobDirectoryServerServiceImpl(
        final ResourceLoader resourceLoader,
        final DataServices dataServices,
        final AgentFileStreamService agentFileStreamService,
        final ArchivedJobService archivedJobService,
        final GenieResourceHandler.Factory genieResourceHandlerFactory,
        final MeterRegistry meterRegistry,
        final AgentRoutingService agentRoutingService
    ) {
        this.resourceLoader = resourceLoader;
        this.persistenceService = dataServices.getPersistenceService();
        this.agentFileStreamService = agentFileStreamService;
        this.meterRegistry = meterRegistry;
        this.genieResourceHandlerFactory = genieResourceHandlerFactory;
        this.archivedJobService = archivedJobService;
        this.agentRoutingService = agentRoutingService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void serveResource(
        final String id,
        final URL baseUrl,
        final String relativePath,
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws GenieException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            // Normalize the base url. Make sure it ends in /.
            final URI baseUri = new URI(baseUrl.toString() + SLASH).normalize();

            // Lookup archive status and job execution type
            final ArchiveStatus archiveStatus = this.persistenceService.getJobArchiveStatus(id);
            tags.add(Tag.of(ARCHIVE_STATUS_TAG, archiveStatus.name()));

            final DirectoryManifest manifest;
            final URI jobDirRoot;

            switch (archiveStatus) {
                case NO_FILES:
                    // Job failed before any files were created. Nothing to serve.
                    throw new GenieNotFoundException("Job failed before any file was created: " + id);

                case FAILED:
                    // Archive failed (also implies job is done). Return 404 without further processing
                    throw new GenieNotFoundException("Job failed to archive files: " + id);

                case DISABLED:
                    // Not a possible state in database as of now [GENIE-657]
                    throw new GeniePreconditionException("Archive disabled for job " + id);

                case UNKNOWN:
                    // Set by the server when an agent is AWOL long enough.
                    // Archive status is truly unknown. As of now, fall-through and attempt serving from archive.

                case ARCHIVED:
                    // Serve file from archive
                    log.debug("Routing request to archive");
                    final ArchivedJobMetadata archivedJobMetadata = this.archivedJobService.getArchivedJobMetadata(id);
                    final String rangeHeader = request.getHeader(HttpHeaders.RANGE);
                    manifest = archivedJobMetadata.getManifest();
                    final URI baseJobDirRoot = archivedJobMetadata.getArchiveBaseUri();
                    jobDirRoot = new URIBuilder(baseJobDirRoot).setFragment(rangeHeader).build();
                    break;

                case PENDING:
                    log.debug("Routing request to connected agent");
                    if (!this.agentRoutingService.isAgentConnectionLocal(id)) {
                        throw new GenieServerUnavailableException("Agent connection has moved or was terminated");
                    }
                    manifest = this.agentFileStreamService.getManifest(id).orElseThrow(
                        () -> new GenieServerUnavailableException("Manifest not found for job " + id)
                    );
                    jobDirRoot = AgentFileProtocolResolver.createUri(
                        id,
                        SLASH,
                        request.getHeader(HttpHeaders.RANGE)
                    );
                    break;

                default:
                    throw new GenieServerException("Unknown archive status " + archiveStatus + "(" + id + ")");
            }

            log.debug(
                "Serving file: {} for job: {} (archive status: {})",
                relativePath,
                id,
                archiveStatus
            );

            // Common handling of archived, locally running v3 job or locally connected v4 job
            this.handleRequest(baseUri, relativePath, request, response, manifest, jobDirRoot);
            MetricsUtils.addSuccessTags(tags);

        } catch (NotFoundException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new GenieNotFoundException(e.getMessage(), e);
        } catch (IOException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new GenieServerException("Error serving response: " + e.getMessage(), e);
        } catch (URISyntaxException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new GenieServerException(e.getMessage(), e);
        } catch (final JobNotArchivedException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new GeniePreconditionException("Job outputs were not archived", e);
        } catch (final JobNotFoundException | JobDirectoryManifestNotFoundException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new GenieNotFoundException("Failed to retrieve job archived files metadata", e);
        } catch (GenieException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } finally {
            final long elapsed = System.nanoTime() - start;
            this.meterRegistry.timer(SERVE_RESOURCE_TIMER, tags).record(elapsed, TimeUnit.NANOSECONDS);
        }
    }

    private void handleRequest(
        final URI baseUri,
        final String relativePath,
        final HttpServletRequest request,
        final HttpServletResponse response,
        final DirectoryManifest manifest,
        final URI jobDirectoryRoot
    ) throws IOException, GenieNotFoundException, GenieServerException {
        log.debug(
            "Handle request, baseUri: '{}', relpath: '{}', jobRootUri: '{}'",
            baseUri,
            relativePath,
            jobDirectoryRoot
        );
        final DirectoryManifest.ManifestEntry entry = manifest.getEntry(relativePath).orElseThrow(
            () -> new GenieNotFoundException("No such entry in job manifest: " + relativePath)
        );

        if (entry.isDirectory()) {
            // For now maintain the V3 structure
            // TODO: Once we determine what we want for V4 use v3/v4 flags or some way to differentiate
            // TODO: there's no unit test covering this section
            final DefaultDirectoryWriter.Directory directory = new DefaultDirectoryWriter.Directory();
            final List<DefaultDirectoryWriter.Entry> files = Lists.newArrayList();
            final List<DefaultDirectoryWriter.Entry> directories = Lists.newArrayList();
            try {
                entry.getParent().ifPresent(
                    parentPath -> {
                        final DirectoryManifest.ManifestEntry parentEntry = manifest
                            .getEntry(parentPath)
                            .orElseThrow(IllegalArgumentException::new);
                        directory.setParent(createEntry(parentEntry, baseUri));
                    }
                );

                for (final String childPath : entry.getChildren()) {
                    final DirectoryManifest.ManifestEntry childEntry = manifest
                        .getEntry(childPath)
                        .orElseThrow(IllegalArgumentException::new);

                    if (childEntry.isDirectory()) {
                        directories.add(this.createEntry(childEntry, baseUri));
                    } else {
                        files.add(this.createEntry(childEntry, baseUri));
                    }
                }
            } catch (final IllegalArgumentException iae) {
                throw new GenieServerException("Error while traversing files manifest: " + iae.getMessage(), iae);
            }

            directories.sort(Comparator.comparing(DefaultDirectoryWriter.Entry::getName));
            files.sort(Comparator.comparing(DefaultDirectoryWriter.Entry::getName));

            directory.setDirectories(directories);
            directory.setFiles(files);

            final String accept = request.getHeader(HttpHeaders.ACCEPT);
            if (accept != null && accept.contains(MediaType.TEXT_HTML_VALUE)) {
                response.setContentType(MediaType.TEXT_HTML_VALUE);
                response
                    .getOutputStream()
                    .write(
                        DefaultDirectoryWriter
                            .directoryToHTML(entry.getName(), directory)
                            .getBytes(StandardCharsets.UTF_8)
                    );
            } else {
                response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                GenieObjectMapper.getMapper().writeValue(response.getOutputStream(), directory);
            }
        } else {
            final URI location = jobDirectoryRoot.resolve(entry.getPath());
            final String locationString = location.toString()
                + (jobDirectoryRoot.getFragment() != null ? ("#" + jobDirectoryRoot.getFragment()) : "");
            log.debug("Get resource: {}", locationString);
            final Resource jobResource = this.resourceLoader.getResource(locationString);
            // Every file really should have a media type but if not use text/plain
            final String mediaType = entry.getMimeType().orElse(MediaType.TEXT_PLAIN_VALUE);
            final ResourceHttpRequestHandler handler = this.genieResourceHandlerFactory.get(mediaType, jobResource);
            try {
                handler.handleRequest(request, response);
            } catch (ServletException e) {
                throw new GenieServerException("Servlet exception: " + e.getMessage(), e);
            }
        }
    }

    private DefaultDirectoryWriter.Entry createEntry(
        final DirectoryManifest.ManifestEntry manifestEntry,
        final URI baseUri
    ) {
        final DefaultDirectoryWriter.Entry entry = new DefaultDirectoryWriter.Entry();
        // For backwards compatibility the V3 names ended in "/" for directories
        if (manifestEntry.isDirectory()) {
            entry.setName(
                manifestEntry.getName().endsWith("/") ? manifestEntry.getName() : manifestEntry.getName() + "/"
            );
        } else {
            entry.setName(manifestEntry.getName());
        }
        entry.setUrl(baseUri.resolve(manifestEntry.getPath()).toString());
        entry.setSize(manifestEntry.getSize());
        entry.setLastModified(manifestEntry.getLastModifiedTime());
        return entry;
    }

    /**
     * Helper class which overrides two entry points from {@link ResourceHttpRequestHandler} in order to be easily
     * reusable for our use case while still leveraging all the work done in there for proper HTTP interaction.
     *
     * @author tgianos
     * @since 4.0.0
     */
    private static class GenieResourceHandler extends ResourceHttpRequestHandler {

        private static final ResourceHttpMessageConverter RESOURCE_HTTP_MESSAGE_CONVERTER
            = new ResourceHttpMessageConverter();
        private static final ResourceRegionHttpMessageConverter RESOURCE_REGION_HTTP_MESSAGE_CONVERTER
            = new ResourceRegionHttpMessageConverter();

        private final MediaType mediaType;
        private final Resource jobResource;

        GenieResourceHandler(final String mediaType, final Resource jobResource) {
            super();
            // TODO: This throws InvalidMediaTypeException. Not sure if should bother handing it here or not seeing
            //       as the mime types were already derived successfully in the manifest creation
            this.mediaType = MediaType.parseMediaType(mediaType);
            this.jobResource = jobResource;

            // Cheat to avoid assertions in the super handleRequest impl due to lack of being in an application context
            this.setResourceHttpMessageConverter(RESOURCE_HTTP_MESSAGE_CONVERTER);
            this.setResourceRegionHttpMessageConverter(RESOURCE_REGION_HTTP_MESSAGE_CONVERTER);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Resource getResource(final HttpServletRequest request) throws IOException {
            return this.jobResource;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected MediaType getMediaType(final HttpServletRequest request, final Resource resource) {
            return this.mediaType;
        }

        /**
         * Simple factory to avoid using 'new' inline, and facilitate mocking and testing.
         */
        private static class Factory {
            ResourceHttpRequestHandler get(final String mediaType, final Resource jobResource) {
                return new GenieResourceHandler(mediaType, jobResource);
            }
        }
    }
}
