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
package com.netflix.genie.web.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Properties for the {@link com.netflix.genie.web.services.AttachmentService}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = AttachmentServiceProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class AttachmentServiceProperties {

    /**
     * The property prefix for job user limiting.
     */
    public static final String PROPERTY_PREFIX = "genie.jobs.attachments";

    private static final Path SYSTEM_TMP_DIR = Paths.get(System.getProperty("java.io.tmpdir", "/tmp/"));

    @NotNull(message = "Attachment location prefix is required")
    private URI locationPrefix = URI.create("file://" + SYSTEM_TMP_DIR.resolve("genie/attachments"));

    @NotNull(message = "Maximum attachment size is required")
    private DataSize maxSize = DataSize.ofMegabytes(100);

    @NotNull(message = "Maximum attachments total size is required")
    private DataSize maxTotalSize = DataSize.ofMegabytes(150);

}
