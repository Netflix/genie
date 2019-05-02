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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.internal.dto.JobDirectoryManifest;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.resources.agent.AgentFileProtocolResolver;
import com.netflix.genie.web.resources.writers.DefaultDirectoryWriter;
import com.netflix.genie.web.services.AgentFileStreamService;
import com.netflix.genie.web.services.JobDirectoryServerService;
import com.netflix.genie.web.services.JobFileService;
import com.netflix.genie.web.services.JobPersistenceService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
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
import java.util.Optional;

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
    private final LoadingCache<String, ManifestCacheValue> manifestCache;
    private final GenieResourceHandler.Factory genieResourceHandlerFactory;

    /**
     * Constructor.
     *
     * @param resourceLoader         The application resource loader used to get references to resources
     * @param jobPersistenceService  The job persistence service used to get information about a job
     * @param jobFileService         The service responsible for managing the job directory for V3 Jobs
     * @param agentFileStreamService The service providing file manifest for active agent jobs
     * @param meterRegistry          The meter registry used to keep track of metrics
     */
    public JobDirectoryServerServiceImpl(
        final ResourceLoader resourceLoader,
        final JobPersistenceService jobPersistenceService,
        final JobFileService jobFileService,
        final AgentFileStreamService agentFileStreamService,
        final MeterRegistry meterRegistry
    ) {
        this(
            resourceLoader,
            jobPersistenceService,
            jobFileService,
            agentFileStreamService,
            meterRegistry,
            new GenieResourceHandler.Factory()
        );
    }

    /**
     * Constructor that accepts a handler factory mock for easier testing.
     */
    @VisibleForTesting
    JobDirectoryServerServiceImpl(
        final ResourceLoader resourceLoader,
        final JobPersistenceService jobPersistenceService,
        final JobFileService jobFileService,
        final AgentFileStreamService agentFileStreamService,
        final MeterRegistry meterRegistry,
        final GenieResourceHandler.Factory genieResourceHandlerFactory
    ) {

        this.resourceLoader = resourceLoader;
        this.jobPersistenceService = jobPersistenceService;
        this.jobFileService = jobFileService;
        this.agentFileStreamService = agentFileStreamService;
        this.meterRegistry = meterRegistry;
        this.genieResourceHandlerFactory = genieResourceHandlerFactory;

        // TODO: This is a local cache. It might be valuable to have a shared cluster cache?
        // TODO: May want to tweak parameters or make them configurable
        // TODO: Should we expire more proactively than just waiting for size to fill up?
        this.manifestCache = CacheBuilder
            .newBuilder()
            .maximumSize(50L)
            .recordStats()
            .build(
                new CacheLoader<String, ManifestCacheValue>() {
                    @Override
                    public ManifestCacheValue load(final String key) throws Exception {
                        // TODO: Probably need more specific exceptions so we can map them to response codes
                        final String archiveLocation = jobPersistenceService
                            .getJobArchiveLocation(key)
                            .orElseThrow(() -> new JobNotArchivedException("Job " + key + " wasn't archived"));

                        final URI jobDirectoryRoot = new URI(archiveLocation + SLASH).normalize();

                        final URI manifestLocation;
                        if (StringUtils.isBlank(JobArchiveService.MANIFEST_DIRECTORY)) {
                            manifestLocation = jobDirectoryRoot.resolve(JobArchiveService.MANIFEST_NAME).normalize();
                        } else {
                            manifestLocation = jobDirectoryRoot
                                .resolve(JobArchiveService.MANIFEST_DIRECTORY + SLASH)
                                .resolve(JobArchiveService.MANIFEST_NAME)
                                .normalize();
                        }

                        final Resource manifestResource = resourceLoader.getResource(manifestLocation.toString());
                        if (!manifestResource.exists()) {
                            throw new GenieNotFoundException("No job manifest exists at " + manifestLocation);
                        }
                        final JobDirectoryManifest manifest = GenieObjectMapper
                            .getMapper()
                            .readValue(manifestResource.getInputStream(), JobDirectoryManifest.class);

                        return new ManifestCacheValue(manifest, jobDirectoryRoot);
                    }
                }
            );

        // TODO: Record metrics for cache using stats() method return
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
    ) throws IOException, ServletException {
        // TODO: Metrics
        // Is the job running or not?
        final JobStatus jobStatus;
        try {
            jobStatus = this.jobPersistenceService.getJobStatus(jobId);
        } catch (final GenieNotFoundException e) {
            log.error(e.getMessage(), e);
            response.sendError(HttpStatus.NOT_FOUND.value(), e.getMessage());
            return;
        }

        // Is it V3 or V4?
        final boolean isV4;
        try {
            isV4 = this.jobPersistenceService.isV4(jobId);
        } catch (final GenieJobNotFoundException nfe) {
            // Really after the last check this shouldn't happen but just in case
            log.error(nfe.getMessage(), nfe);
            response.sendError(HttpStatus.NOT_FOUND.value(), nfe.getMessage());
            return;
        }

        // Normalize the base url. Make sure it ends in /.
        final URI baseUri;
        try {
            baseUri = new URI(baseUrl.toString() + SLASH).normalize();
        } catch (final URISyntaxException e) {
            log.error(e.getMessage(), e);
            response.sendError(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                "Unable to convert " + baseUrl + " to valid URI"
            );
            return;
        }

        if (jobStatus.isActive() && isV4) {

            final Optional<JobDirectoryManifest> manifest = this.agentFileStreamService.getManifest(jobId);
            if (!manifest.isPresent()) {
                log.error("Manifest not found for active job: {}", jobId);
                response.sendError(HttpStatus.SERVICE_UNAVAILABLE.value(), "Could not load manifest for job: " + jobId);
                return;
            }

            final URI jobDirRoot;
            try {
                jobDirRoot = new URI(AgentFileProtocolResolver.URI_SCHEME, jobId, SLASH, null);
            } catch (final URISyntaxException e) {
                log.error(e.getMessage(), e);
                response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getMessage());
                return;
            }

            this.handleRequest(baseUri, relativePath, request, response, manifest.get(), jobDirRoot);


        } else if (jobStatus.isActive()) {
            // Active V3 job

            // TODO: Manifest creation could be expensive
            final Resource jobDir = this.jobFileService.getJobFileAsResource(jobId, "");
            if (!jobDir.exists()) {
                log.error("Job directory {} doesn't exist. Unable to serve job contents.", jobDir);
                response.sendError(HttpStatus.NOT_FOUND.value());
                return;
            }
            final URI jobDirRoot;
            try {
                // Make sure the directory ends in a slash. Normalize will ensure only single slash
                jobDirRoot = new URI(jobDir.getURI().toString() + SLASH).normalize();
            } catch (final URISyntaxException e) {
                log.error(e.getMessage(), e);
                response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getMessage());
                return;
            }
            final Path jobDirPath = Paths.get(jobDirRoot);

            final JobDirectoryManifest manifest = new JobDirectoryManifest(jobDirPath, false);
            this.handleRequest(baseUri, relativePath, request, response, manifest, jobDirRoot);
        } else {
            // Archived job
            final JobDirectoryManifest manifest;
            final URI jobDirRoot;
            try {
                final ManifestCacheValue cacheValue = this.manifestCache.get(jobId);
                manifest = cacheValue.getManifest();
                jobDirRoot = cacheValue.getJobDirectoryRoot();
            } catch (final Exception e) {
                // TODO: more fine grained exception handling
                if (e.getCause() instanceof JobNotArchivedException) {
                    // will be thrown from the manifest loader
                    log.error(e.getCause().getMessage(), e.getCause());
                    response.sendError(HttpStatus.NOT_FOUND.value(), e.getCause().getMessage());
                } else {
                    log.error(e.getMessage(), e);
                    response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getMessage());
                }
                return;
            }
            this.handleRequest(baseUri, relativePath, request, response, manifest, jobDirRoot);
        }
    }

    private void handleRequest(
        final URI baseUri,
        final String relativePath,
        final HttpServletRequest request,
        final HttpServletResponse response,
        final JobDirectoryManifest manifest,
        final URI jobDirectoryRoot
    ) throws IOException, ServletException {
        log.debug(
            "Handle request, baseUri: '{}', relpath: '{}', jobRootUri: '{}'",
            baseUri,
            relativePath,
            jobDirectoryRoot
        );
        final JobDirectoryManifest.ManifestEntry entry;
        final Optional<JobDirectoryManifest.ManifestEntry> entryOptional = manifest.getEntry(relativePath);
        if (entryOptional.isPresent()) {
            entry = entryOptional.get();
        } else {
            log.error("No such entry in job manifest: {}", relativePath);
            response.sendError(HttpStatus.NOT_FOUND.value(), "Not found: " + relativePath);
            return;
        }

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
                        final JobDirectoryManifest.ManifestEntry parentEntry = manifest
                            .getEntry(parentPath)
                            .orElseThrow(IllegalArgumentException::new);
                        directory.setParent(createEntry(parentEntry, baseUri));
                    }
                );

                for (final String childPath : entry.getChildren()) {
                    final JobDirectoryManifest.ManifestEntry childEntry = manifest
                        .getEntry(childPath)
                        .orElseThrow(IllegalArgumentException::new);

                    if (childEntry.isDirectory()) {
                        directories.add(this.createEntry(childEntry, baseUri));
                    } else {
                        files.add(this.createEntry(childEntry, baseUri));
                    }
                }
            } catch (final IllegalArgumentException iae) {
                log.error("Encountered unexpected problem traversing the manifest for directory entry {}", entry, iae);
                response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
                return;
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
            handler.handleRequest(request, response);
        }
    }

    private DefaultDirectoryWriter.Entry createEntry(
        final JobDirectoryManifest.ManifestEntry manifestEntry,
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

    /**
     * A simple POJO for a compound value of related information to a job to store in a cache.
     *
     * @author tgianos
     * @see CacheLoader#load(Object)
     * @since 4.0.0
     */
    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode(doNotUseGetters = true)
    @ToString(doNotUseGetters = true)
    private static class ManifestCacheValue {
        private final JobDirectoryManifest manifest;
        private final URI jobDirectoryRoot;
    }

    /**
     * Simple exception to represent when a job wasn't archived so it's impossible to get the output.
     *
     * @author tgianos
     * @since 4.0.0
     */
    private static class JobNotArchivedException extends RuntimeException {
        JobNotArchivedException(final String message) {
            super(message);
        }
    }
}
