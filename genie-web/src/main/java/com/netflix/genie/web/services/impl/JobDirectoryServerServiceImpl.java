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
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.dtos.DirectoryManifest;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import com.netflix.genie.web.agent.resources.AgentFileProtocolResolver;
import com.netflix.genie.web.agent.services.AgentFileStreamService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.dtos.ArchivedJobMetadata;
import com.netflix.genie.web.exceptions.checked.JobDirectoryManifestNotFoundException;
import com.netflix.genie.web.exceptions.checked.JobNotArchivedException;
import com.netflix.genie.web.exceptions.checked.JobNotFoundException;
import com.netflix.genie.web.resources.writers.DefaultDirectoryWriter;
import com.netflix.genie.web.services.ArchivedJobService;
import com.netflix.genie.web.services.JobDirectoryServerService;
import com.netflix.genie.web.services.JobFileService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

/**
 * Default implementation of {@link JobDirectoryServerService}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class JobDirectoryServerServiceImpl implements JobDirectoryServerService {

    private static final String SLASH = "/";

    private final ResourceLoader resourceLoader;
    private final JobPersistenceService jobPersistenceService;
    private final JobFileService jobFileService;
    private final AgentFileStreamService agentFileStreamService;
    private final MeterRegistry meterRegistry;
    private final GenieResourceHandler.Factory genieResourceHandlerFactory;
    private final JobDirectoryManifestCreatorService jobDirectoryManifestCreatorService;
    private final ArchivedJobService archivedJobService;

    /**
     * Constructor.
     *
     * @param resourceLoader                     The application resource loader used to get references to resources
     * @param jobPersistenceService              The job persistence service used to get information about a job
     * @param agentFileStreamService             The service providing file manifest for active agent jobs
     * @param archivedJobService                 The {@link ArchivedJobService} implementation to use to get archived
     *                                           job data
     * @param meterRegistry                      The meter registry used to keep track of metrics
     * @param jobFileService                     The service responsible for managing the job directory for V3 Jobs
     * @param jobDirectoryManifestCreatorService The job directory manifest service
     */
    public JobDirectoryServerServiceImpl(
        final ResourceLoader resourceLoader,
        final JobPersistenceService jobPersistenceService,
        final AgentFileStreamService agentFileStreamService,
        final ArchivedJobService archivedJobService,
        final MeterRegistry meterRegistry,
        final JobFileService jobFileService,
        final JobDirectoryManifestCreatorService jobDirectoryManifestCreatorService
    ) {
        this(
            resourceLoader,
            jobPersistenceService,
            agentFileStreamService,
            archivedJobService,
            new GenieResourceHandler.Factory(),
            meterRegistry,
            jobFileService,
            jobDirectoryManifestCreatorService
        );
    }

    /**
     * Constructor that accepts a handler factory mock for easier testing.
     */
    @VisibleForTesting
    JobDirectoryServerServiceImpl(
        final ResourceLoader resourceLoader,
        final JobPersistenceService jobPersistenceService,
        final AgentFileStreamService agentFileStreamService,
        final ArchivedJobService archivedJobService,
        final GenieResourceHandler.Factory genieResourceHandlerFactory,
        final MeterRegistry meterRegistry,
        final JobFileService jobFileService,
        final JobDirectoryManifestCreatorService jobDirectoryManifestCreatorService
    ) {
        this.resourceLoader = resourceLoader;
        this.jobPersistenceService = jobPersistenceService;
        this.jobFileService = jobFileService;
        this.agentFileStreamService = agentFileStreamService;
        this.meterRegistry = meterRegistry;
        this.genieResourceHandlerFactory = genieResourceHandlerFactory;
        this.jobDirectoryManifestCreatorService = jobDirectoryManifestCreatorService;
        this.archivedJobService = archivedJobService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void serveResource(
        final String jobId,
        final URL baseUrl,
        final String relativePath,
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws GenieException {
        // TODO: Metrics
        // Is the job running or not?
        final JobStatus jobStatus = this.jobPersistenceService.getJobStatus(jobId);
        // Is it V3 or V4?
        final boolean isV4 = this.jobPersistenceService.isV4(jobId);

        // Normalize the base url. Make sure it ends in /.
        final URI baseUri;
        try {
            baseUri = new URI(baseUrl.toString() + SLASH).normalize();
        } catch (final URISyntaxException e) {
            throw new GenieServerException("Unable to convert " + baseUrl + " to valid URI", e);
        }

        final DirectoryManifest manifest;
        final URI jobDirRoot;

        if (jobStatus.isActive() && isV4) { // Active V4 job
            manifest = this.agentFileStreamService.getManifest(jobId).orElseThrow(
                () -> new GenieServerUnavailableException("Manifest not found for job " + jobId)
            );
            try {
                jobDirRoot = AgentFileProtocolResolver.createUri(jobId, SLASH);
            } catch (final URISyntaxException e) {
                throw new GenieServerException("Failed to construct job directory path", e);
            }
        } else if (jobStatus.isActive()) { // Active V3 job
            final Resource jobDir = this.jobFileService.getJobFileAsResource(jobId, "");
            if (!jobDir.exists()) {
                throw new GenieNotFoundException("Job directory does not exist: " + jobDir);
            }
            try {
                // Make sure the directory ends in a slash. Normalize will ensure only single slash
                jobDirRoot = new URI(jobDir.getURI().toString() + SLASH).normalize();
            } catch (final URISyntaxException | IOException e) {
                throw new GenieServerException("Failed to normalize job directory path", e);
            }
            final Path jobDirPath = Paths.get(jobDirRoot);

            try {
                manifest = this.jobDirectoryManifestCreatorService.getDirectoryManifest(jobDirPath);
            } catch (IOException e) {
                throw new GenieServerException("Failed to construct manifest: " + e.getMessage(), e);
            }
        } else { // Archived job
            try {
                final ArchivedJobMetadata archivedJobMetadata = this.archivedJobService.getArchivedJobMetadata(jobId);
                manifest = archivedJobMetadata.getManifest();
                jobDirRoot = archivedJobMetadata.getArchiveBaseUri();
            } catch (final JobNotArchivedException e) {
                throw new GeniePreconditionException("Job outputs were not archived", e);
            } catch (final JobNotFoundException | JobDirectoryManifestNotFoundException e) {
                throw new GenieNotFoundException("Failed to retrieve job archived files metadata", e);
            } catch (final Exception e) {
                throw new GenieServerException("Error job metadata: " + e.getMessage(), e);
            }
        }

        // Common handling of
        try {
            this.handleRequest(baseUri, relativePath, request, response, manifest, jobDirRoot);
        } catch (IOException e) {
            throw new GenieServerException("Error serving response: " + e.getMessage(), e);
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
            log.debug("Get resource: {}", location);
            final Resource jobResource = this.resourceLoader.getResource(location.toString());
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
