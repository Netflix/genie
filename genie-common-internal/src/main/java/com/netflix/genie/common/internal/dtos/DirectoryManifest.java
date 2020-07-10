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
package com.netflix.genie.common.internal.dtos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystemLoopException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * A manifest of all the files and subdirectories in a directory.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ToString(doNotUseGetters = true)
@EqualsAndHashCode(doNotUseGetters = true)
public class DirectoryManifest {
    private static final String ENTRIES_KEY = "entries";
    private static final String EMPTY_STRING = "";

    private final ImmutableMap<String, ManifestEntry> entries;
    private final ImmutableSet<ManifestEntry> files;
    private final ImmutableSet<ManifestEntry> directories;
    private final int numFiles;
    private final int numDirectories;
    private final long totalSizeOfFiles;

    private DirectoryManifest(
        final Path directory,
        final boolean calculateFileChecksums,
        final Filter filter
    ) throws IOException {
        // Walk the directory
        final ImmutableMap.Builder<String, ManifestEntry> builder = ImmutableMap.builder();
        final ManifestVisitor manifestVisitor = new ManifestVisitor(
            directory,
            builder,
            calculateFileChecksums,
            filter
        );
        final EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        Files.walkFileTree(directory, options, Integer.MAX_VALUE, manifestVisitor);
        this.entries = builder.build();

        final ImmutableSet.Builder<ManifestEntry> filesBuilder = ImmutableSet.builder();
        final ImmutableSet.Builder<ManifestEntry> directoriesBuilder = ImmutableSet.builder();

        long sizeOfFiles = 0L;
        for (final ManifestEntry entry : this.entries.values()) {
            if (entry.isDirectory()) {
                directoriesBuilder.add(entry);
            } else {
                filesBuilder.add(entry);
                sizeOfFiles += entry.getSize();
            }
        }

        this.totalSizeOfFiles = sizeOfFiles;
        this.directories = directoriesBuilder.build();
        this.files = filesBuilder.build();
        this.numDirectories = this.directories.size();
        this.numFiles = this.files.size();
    }

    /**
     * Create a manifest from an existing set of entries. Generally this should be used to regenerate an in memory
     * manifest instance from JSON.
     *
     * @param entries The entries in this manifest
     */
    @JsonCreator
    public DirectoryManifest(
        @JsonProperty(value = ENTRIES_KEY, required = true) final Set<ManifestEntry> entries
    ) {
        final ImmutableMap.Builder<String, ManifestEntry> builder = ImmutableMap.builder();
        final ImmutableSet.Builder<ManifestEntry> filesBuilder = ImmutableSet.builder();
        final ImmutableSet.Builder<ManifestEntry> directoriesBuilder = ImmutableSet.builder();

        long sizeOfFiles = 0L;
        for (final ManifestEntry entry : entries) {
            builder.put(entry.getPath(), entry);
            if (entry.isDirectory()) {
                directoriesBuilder.add(entry);
            } else {
                filesBuilder.add(entry);
                sizeOfFiles += entry.getSize();
            }
        }
        this.entries = builder.build();
        this.totalSizeOfFiles = sizeOfFiles;
        this.directories = directoriesBuilder.build();
        this.files = filesBuilder.build();
        this.numDirectories = this.directories.size();
        this.numFiles = this.files.size();
    }

    /**
     * Check whether an entry exists for the given path.
     *
     * @param path The path to check. Relative to the root of the original job directory.
     * @return {@code true} if an entry exists for this path
     */
    public boolean hasEntry(final String path) {
        return this.entries.containsKey(path);
    }

    /**
     * Get the entry, if one exists, for the given path.
     *
     * @param path The path to get an entry for. Relative to the root of the original job directory.
     * @return The entry wrapped in an {@link Optional} or {@link Optional#empty()} if no entry exists
     */
    @JsonIgnore
    public Optional<ManifestEntry> getEntry(final String path) {
        return Optional.ofNullable(this.entries.get(path));
    }

    /**
     * A getter used to mask internal implementation for JSON serialization.
     *
     * @return All the entries as a collection.
     */
    @JsonGetter(ENTRIES_KEY)
    Collection<ManifestEntry> getEntries() {
        return this.entries.values();
    }

    /**
     * Get all the entries that are files for this manifest.
     *
     * @return All the file {@link ManifestEntry}'s as an immutable set.
     */
    @JsonIgnore
    public Set<ManifestEntry> getFiles() {
        return this.files;
    }

    /**
     * Get all the entries that are directories for this manifest.
     *
     * @return All the directory {@link ManifestEntry}'s as an immutable set.
     */
    @JsonIgnore
    public Set<ManifestEntry> getDirectories() {
        return this.directories;
    }

    /**
     * Get the total number of files in this manifest.
     *
     * @return The total number of files that are in this job directory
     */
    @JsonIgnore
    public int getNumFiles() {
        return this.numFiles;
    }

    /**
     * Get the total number of directories in this manifest.
     *
     * @return The total number of sub directories that are in this job directory
     */
    @JsonIgnore
    public int getNumDirectories() {
        return this.numDirectories;
    }

    /**
     * Get the total size of the files contained in this manifest.
     *
     * @return The total size (in bytes) of all the files in this job directory
     */
    @JsonIgnore
    public long getTotalSizeOfFiles() {
        return this.totalSizeOfFiles;
    }

    /**
     * This interface defines a filter function used during creation of the manifest.
     * It can prune entire sub-trees of the directory, optionally including the directory itself, or skip individual
     * files.
     * The default implementation accepts all entries and filters none.
     */
    public interface Filter {

        /**
         * Whether to include a given file in the manifest.
         *
         * @param filePath the file path
         * @param attrs    the file attributes
         * @return true if the file should be included in the manifest, false if it should be excluded.
         */
        default boolean includeFile(final Path filePath, BasicFileAttributes attrs) {
            return true;
        }

        /**
         * Whether to include a given directory in the manifest. If a directory is not included, all sub-directories
         * and files contained are also implicitly excluded.
         *
         * @param dirPath the directory path
         * @param attrs   the directory attributes
         * @return true if the directory should be included in the manifest, false if it should be excluded.
         */
        default boolean includeDirectory(final Path dirPath, BasicFileAttributes attrs) {
            return true;
        }

        /**
         * Whether to recurse into a given directory and add its contents to the manifest. Only evaluated if the
         * directory is not excluded.
         *
         * @param dirPath the directory path
         * @param attrs   the directory attributes
         * @return true if the contents of the directory should be included in the manifest, false if they should be
         * excluded.
         */
        default boolean walkDirectory(final Path dirPath, BasicFileAttributes attrs) {
            return true;
        }
    }

    /**
     * Factory that encapsulates directory manifest creation.
     */
    public static class Factory {

        private static final Filter ACCEPT_ALL_FILTER = new DirectoryManifest.Filter() {
        };
        private final Filter filter;

        /**
         * Constructor with no filters.
         */
        public Factory() {
            this(ACCEPT_ALL_FILTER);
        }

        /**
         * Constructor with filter.
         *
         * @param filter the manifest filter
         */
        public Factory(final Filter filter) {
            this.filter = filter;
        }

        /**
         * Create a manifest from the given job directory.
         *
         * @param directory       The job directory to create a manifest from
         * @param includeChecksum Whether or not to calculate checksums for each file added to the manifest
         * @return a directory manifest
         * @throws IOException If there is an error reading the directory
         */
        public DirectoryManifest getDirectoryManifest(
            final Path directory,
            final boolean includeChecksum
        ) throws IOException {
            return new DirectoryManifest(directory, includeChecksum, this.filter);
        }
    }

    @Slf4j
    private static class ManifestVisitor extends SimpleFileVisitor<Path> {

        private final Path root;
        private final ImmutableMap.Builder<String, ManifestEntry> builder;
        private final Metadata metadata;
        private final TikaConfig tikaConfig;
        private final boolean checksumFiles;
        private final Filter filter;

        ManifestVisitor(
            final Path root,
            final ImmutableMap.Builder<String, ManifestEntry> builder,
            final boolean checksumFiles,
            final Filter filter
        ) throws IOException {
            this.root = root;
            this.builder = builder;
            this.checksumFiles = checksumFiles;
            this.filter = filter;
            this.metadata = new Metadata();
            try {
                this.tikaConfig = new TikaConfig();
            } catch (final TikaException te) {
                log.error("Unable to create Tika Configuration due to error", te);
                throw new IOException(te);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            final ManifestEntry entry = this.buildEntry(dir, attrs, true);
            if (this.filter.includeDirectory(dir, attrs)) {
                this.builder.put(entry.getPath(), entry);
                log.debug("Created manifest entry for directory {}", entry);
                if (this.filter.walkDirectory(dir, attrs)) {
                    return FileVisitResult.CONTINUE;
                }
            }
            log.debug("Skipping directory: {}", dir.toAbsolutePath());
            return FileVisitResult.SKIP_SUBTREE;

        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            if (this.filter.includeFile(file, attrs)) {
                final ManifestEntry entry = this.buildEntry(file, attrs, false);
                log.debug("Created manifest entry for file {}", entry);
                this.builder.put(entry.getPath(), entry);
            } else {
                log.debug("Skipped manifest entry for file {}", file.toAbsolutePath());
            }

            return FileVisitResult.CONTINUE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult visitFileFailed(final Path file, final IOException ioe) {
            if (ioe instanceof FileSystemLoopException) {
                log.warn("Detected file system cycle visiting while visiting {}. Skipping.", file);
                return FileVisitResult.SKIP_SUBTREE;
            } else if (ioe instanceof AccessDeniedException) {
                log.warn("Access denied for file {}. Skipping", file);
                return FileVisitResult.SKIP_SUBTREE;
            } else if (ioe instanceof NoSuchFileException) {
                log.warn("File or directory disappeared while visiting {}. Skipping", file);
                return FileVisitResult.SKIP_SUBTREE;
            } else {
                log.error("Got unknown error {} while visiting {}. Terminating visitor", ioe.getMessage(), file, ioe);
                // TODO: Not sure if we should do this or skip subtree or just continue and ignore it?
                return FileVisitResult.TERMINATE;
            }
        }

        @SuppressFBWarnings(
            value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "https://github.com/spotbugs/spotbugs/issues/756"
        )
        private ManifestEntry buildEntry(
            final Path entry,
            final BasicFileAttributes attributes,
            final boolean directory
        ) throws IOException {
            final String path = this.root.relativize(entry).toString();
            final Path fileName = entry.getFileName();
            final String name = fileName == null
                ? EMPTY_STRING
                : fileName.toString();
            final Instant lastModifiedTime = attributes.lastModifiedTime().toInstant();
            final Instant lastAccessTime = attributes.lastAccessTime().toInstant();
            final Instant creationTime = attributes.creationTime().toInstant();
            final long size = attributes.size();

            String md5 = null;
            String mimeType = null;
            if (!directory) {
                if (this.checksumFiles) {
                    try (InputStream data = Files.newInputStream(entry, StandardOpenOption.READ)) {
                        md5 = DigestUtils.md5Hex(data);
                    } catch (final IOException ioe) {
                        // For now MD5 isn't critical or required so we'll swallow errors here
                        log.error("Unable to create MD5 for {} due to error", entry, ioe);
                    }
                }

                mimeType = this.getMimeType(name, entry);
            }

            final Set<String> children = Sets.newHashSet();
            if (directory) {
                try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(entry)) {
                    for (final Path child : directoryStream) {
                        children.add(this.root.relativize(child).toString());
                    }
                }
            }

            String parent = null;
            if (StringUtils.isNotEmpty(path)) {
                // Not the root
                parent = this.root.relativize(entry.getParent()).toString();
            }

            return new ManifestEntry(
                path,
                name,
                lastModifiedTime,
                lastAccessTime,
                creationTime,
                directory,
                size,
                md5,
                mimeType,
                parent,
                children
            );
        }

        private String getMimeType(final String name, final Path path) {
            // TODO: Move configuration of special handling cases to external configuration for flexibility
            //       probably a map of filename -> type or extension -> type or produced mime-type -> desired mime-type
            switch (name) {
                case "stdout":
                case "stderr":
                case "run":
                    return MediaType.TEXT_PLAIN.toString();
                default:
                    try (TikaInputStream inputStream = TikaInputStream.get(path)) {
                        return this.tikaConfig.getDetector().detect(inputStream, this.metadata).toString();
                    } catch (final IOException ioe) {
                        log.error("Unable to detect mime type for {} due to error", path, ioe);
                        return MediaType.OCTET_STREAM.toString();
                    }
            }
        }
    }

    /**
     * Representation of the metadata for a job file on a given underlying storage system.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Getter
    @ToString(doNotUseGetters = true)
    @EqualsAndHashCode(doNotUseGetters = true)
    public static class ManifestEntry {
        private final String path;
        private final String name;
        private final Instant lastModifiedTime;
        private final Instant lastAccessTime;
        private final Instant creationTime;
        private final boolean directory;
        @Min(value = 0L, message = "A file can't have a negative size")
        private final long size;
        private final String md5;
        private final String mimeType;
        private final String parent;
        private final Set<String> children;

        /**
         * Constructor.
         *
         * @param path             The relative path to the entry from the root of the job directory
         * @param name             The name of the entry
         * @param lastModifiedTime The time the entry was last modified
         * @param lastAccessTime   The time the entry was last accessed
         * @param creationTime     The time the entry was created
         * @param directory        Whether this entry is a directory or not
         * @param size             The current size of the entry within the storage system in bytes. Min 0
         * @param md5              The md5 hex of the file contents if it's not a directory
         * @param mimeType         The mime type of the file. Null if its a directory
         * @param parent           Optional entry for the path of this entries parent relative to root
         * @param children         The set of paths, from the root, representing children of this entry if any
         */
        @JsonCreator
        public ManifestEntry(
            @JsonProperty(value = "path", required = true) final String path,
            @JsonProperty(value = "name", required = true) final String name,
            @JsonProperty(value = "lastModifiedTime", required = true) final Instant lastModifiedTime,
            @JsonProperty(value = "lastAccessTime", required = true) final Instant lastAccessTime,
            @JsonProperty(value = "creationTime", required = true) final Instant creationTime,
            @JsonProperty(value = "directory", required = true) final boolean directory,
            @JsonProperty(value = "size", required = true) final long size,
            @JsonProperty(value = "md5") @Nullable final String md5,
            @JsonProperty(value = "mimeType") @Nullable final String mimeType,
            @JsonProperty(value = "parent") @Nullable final String parent,
            @JsonProperty(value = "children", required = true) final Set<String> children
        ) {
            this.path = path;
            this.name = name;
            this.lastModifiedTime = lastModifiedTime;
            this.lastAccessTime = lastAccessTime;
            this.creationTime = creationTime;
            this.directory = directory;
            this.size = size;
            this.md5 = md5;
            this.mimeType = mimeType;
            this.parent = parent;
            this.children = ImmutableSet.copyOf(children);
        }

        /**
         * Get the MD5 hash of the file (as 32 hex characters) if it was calculated.
         *
         * @return The MD5 value or {@link Optional#empty()}
         */
        public Optional<String> getMd5() {
            return Optional.ofNullable(this.md5);
        }

        /**
         * Get the mime type of this file if it was calculated.
         *
         * @return The mime type value or {@link Optional#empty()}
         */
        public Optional<String> getMimeType() {
            return Optional.ofNullable(this.mimeType);
        }

        /**
         * Get the relative path from root of the parent of this entry if there was one.
         * There likely wouldn't be one for the root of the job directory.
         *
         * @return The relative path from root of the parent wrapped in an {@link Optional}
         */
        public Optional<String> getParent() {
            return Optional.ofNullable(this.parent);
        }
    }
}
