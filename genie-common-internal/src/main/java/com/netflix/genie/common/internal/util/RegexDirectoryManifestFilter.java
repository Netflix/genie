/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.common.internal.util;

import com.netflix.genie.common.internal.dtos.DirectoryManifest;
import com.netflix.genie.common.internal.properties.RegexDirectoryManifestProperties;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Implementation of DirectoryManifestFilter that filters manifest entries base on a list of regular expressions
 * provided via properties class.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class RegexDirectoryManifestFilter implements DirectoryManifest.Filter {

    private final List<Pattern> filePathPatterns;
    private final List<Pattern> directoryPathPatterns;
    private final List<Pattern> directoryContentPathPatterns;

    /**
     * Constructor.
     *
     * @param properties the regex properties
     */
    public RegexDirectoryManifestFilter(final RegexDirectoryManifestProperties properties) {

        int flags = 0;
        if (!properties.isCaseSensitiveMatching()) {
            flags += Pattern.CASE_INSENSITIVE;
        }

        this.filePathPatterns = compileRegexList(properties.getFileRejectPatterns(), flags);
        this.directoryPathPatterns = compileRegexList(properties.getDirectoryRejectPatterns(), flags);
        this.directoryContentPathPatterns = compileRegexList(properties.getDirectoryTraversalRejectPatterns(), flags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean includeFile(final Path path, final BasicFileAttributes attrs) {
        final String pathString = path.toString();
        return this.filePathPatterns.stream().noneMatch(p -> p.matcher(pathString).matches());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean includeDirectory(final Path path, final BasicFileAttributes attrs) {
        final String pathString = path.toString();
        return this.directoryPathPatterns.stream().noneMatch(p -> p.matcher(pathString).matches());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean walkDirectory(final Path path, final BasicFileAttributes attrs) {
        final String pathString = path.toString();
        return this.directoryContentPathPatterns.stream().noneMatch(p -> p.matcher(pathString).matches());
    }


    private List<Pattern> compileRegexList(final Collection<String> patterns, final int flags) {
        return patterns.stream()
            .map(p -> Pattern.compile(p, flags))
            .collect(Collectors.toList());
    }
}
