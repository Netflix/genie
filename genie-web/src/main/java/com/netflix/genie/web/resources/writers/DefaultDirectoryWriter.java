/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.resources.writers;

import com.google.common.collect.Lists;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.catalina.util.ServerInfo;
import org.apache.catalina.util.TomcatCSS;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.URL;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;

/**
 * A default directory writer implementation.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class DefaultDirectoryWriter implements DirectoryWriter {

    /**
     * Given the provided information render as an HTML string.
     *
     * @param directoryName The name of the directory
     * @param directory     The directory information to serialize
     * @return A string of valid HTML
     * @see org.apache.catalina.servlets.DefaultServlet
     */
    public static String directoryToHTML(final String directoryName, final Directory directory) {
        final StringBuilder builder = new StringBuilder();

        // Render the page header
        builder.append("<!DOCTYPE html>");
        builder.append("<html>");
        builder.append("<head>");
        builder.append("<title>");
        builder.append(directoryName);
        builder.append("</title>");
        builder.append("<style type=\"text/css\"><!--");
        builder.append(TomcatCSS.TOMCAT_CSS);
        builder.append("--></style> ");
        builder.append("</head>");

        // Body
        builder.append("<body>");
        builder.append("<h1>").append(directoryName).append("</h1>");

        builder.append("<HR size=\"1\" noshade=\"noshade\">");

        builder.append("<table width=\"100%\" cellspacing=\"0\"" + " cellpadding=\"5\" align=\"center\">");

        // Render the column headings
        builder.append("<tr>");
        builder.append("<td align=\"left\"><font size=\"+1\"><strong>");
        builder.append("Filename");
        builder.append("</strong></font></td>");
        builder.append("<td align=\"right\"><font size=\"+1\"><strong>");
        builder.append("Size");
        builder.append("</strong></font></td>");
        builder.append("<td align=\"right\"><font size=\"+1\"><strong>");
        builder.append("Last Modified");
        builder.append("</strong></font></td>");
        builder.append("</tr>");

        // Write parent if necessary
        if (directory.getParent() != null) {
            writeFileHtml(builder, false, directory.getParent(), true);
        }

        boolean shade = true;

        // Write directories
        if (directory.getDirectories() != null) {
            for (final Entry entry : directory.getDirectories()) {
                writeFileHtml(builder, shade, entry, true);
                shade = !shade;
            }
        }

        // Write files
        if (directory.getFiles() != null) {
            for (final Entry entry : directory.getFiles()) {
                writeFileHtml(builder, shade, entry, false);
                shade = !shade;
            }
        }

        // Render the page footer
        builder.append("</table>");

        builder.append("<HR size=\"1\" noshade=\"noshade\">");
        // TODO: replace with something related to Genie
        builder.append("<h3>").append(ServerInfo.getServerInfo()).append("</h3>");
        builder.append("</body>");
        builder.append("</html>");

        return builder.toString();
    }

    private static void writeFileHtml(
        final StringBuilder builder,
        final boolean shade,
        final Entry entry,
        final boolean isDirectory
    ) {
        builder.append("<tr");
        if (shade) {
            builder.append(" bgcolor=\"#eeeeee\"");
        }
        builder.append(">");

        builder.append("<td align=\"left\">&nbsp;&nbsp;");
        builder.append("<a href=\"").append(entry.getUrl()).append("\">");
        builder.append("<tt>").append(entry.getName()).append("</tt></a></td>");
        builder.append("<td align=\"right\"><tt>");
        if (isDirectory) {
            builder.append("-");
        } else {
            builder.append(FileUtils.byteCountToDisplaySize(entry.getSize()));
        }
        builder.append("</tt></td>");
        final String lastModified = DateTimeFormatter
            .RFC_1123_DATE_TIME
            .format(entry.getLastModified().atOffset(ZoneOffset.UTC));
        builder.append("<td align=\"right\"><tt>").append(lastModified).append("</tt></td>");
        builder.append("</tr>");
    }

    /**
     * {@inheritDoc}
     *
     * @see org.apache.catalina.servlets.DefaultServlet
     */
    @Override
    public String toHtml(
        @NotNull final File directory,
        @URL final String requestURL,
        final boolean includeParent
    ) throws IOException {
        final Directory dir = this.getDirectory(directory, requestURL, includeParent);
        return directoryToHTML(directory.getName(), dir);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toJson(
        @NotNull final File directory,
        @URL final String requestURL,
        final boolean includeParent
    ) throws Exception {
        final Directory dir = this.getDirectory(directory, requestURL, includeParent);
        return GenieObjectMapper.getMapper().writeValueAsString(dir);
    }

    protected Directory getDirectory(final File directory, final String requestUrl, final boolean includeParent) {
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("Input directory is not a valid directory. Unable to continue.");
        }
        if (StringUtils.isBlank(requestUrl)) {
            throw new IllegalArgumentException("No request url entered. Unable to continue.");
        }
        final Directory dir = new Directory();

        if (includeParent) {
            final Entry parent = new Entry();
            String url = requestUrl;
            if (url.charAt(url.length() - 1) == '/') {
                url = url.substring(0, url.length() - 2);
            }
            // Rip off the last directory
            url = url.substring(0, url.lastIndexOf('/'));
            parent.setName("../");
            parent.setUrl(url);
            parent.setSize(0L);
            parent.setLastModified(Instant.ofEpochMilli(directory.getParentFile().getAbsoluteFile().lastModified()));
            dir.setParent(parent);
        }

        final File[] files = directory.listFiles();
        dir.setDirectories(Lists.newArrayList());
        dir.setFiles(Lists.newArrayList());
        final String baseURL = requestUrl.endsWith("/") ? requestUrl : requestUrl + "/";
        if (files != null) {
            for (final File file : files) {
                final Entry entry = new Entry();
                entry.setLastModified(Instant.ofEpochMilli(file.getAbsoluteFile().lastModified()));
                if (file.isDirectory()) {
                    entry.setName(file.getName() + "/");
                    entry.setUrl(baseURL + file.getName() + "/");
                    entry.setSize(0L);
                    dir.getDirectories().add(entry);
                } else {
                    entry.setName(file.getName());
                    entry.setUrl(baseURL + file.getName());
                    entry.setSize(file.getAbsoluteFile().length());
                    dir.getFiles().add(entry);
                }
            }
        }

        dir.getDirectories().sort(
            Comparator.comparing(Entry::getName)
        );

        dir.getFiles().sort(
            Comparator.comparing(Entry::getName)
        );

        return dir;
    }

    /**
     * DTO for representing a directory contents.
     *
     * @author tgianos
     * @since 3.0.0
     */
    @Data
    public static class Directory {
        private Entry parent;
        private List<Entry> directories;
        private List<Entry> files;
    }

    /**
     * DTO for representing information about an entry within a job directory.
     *
     * @author tgianos
     * @since 3.0.0
     */
    @Data
    @EqualsAndHashCode(exclude = {"lastModified"})
    public static class Entry {
        @NotBlank
        private String name;
        @URL
        private String url;
        @Min(0)
        private long size;
        @NotNull
        private Instant lastModified;
    }
}
