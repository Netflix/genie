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
package com.netflix.genie.common.internal.properties;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import java.util.Set;

/**
 * Properties for {@link com.netflix.genie.common.internal.util.RegexDirectoryManifestFilter}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = RegexDirectoryManifestProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class RegexDirectoryManifestProperties {
    /**
     * Properties prefix.
     */
    public static final String PROPERTY_PREFIX = "genie.jobs.files.filter";

    private boolean caseSensitiveMatching = true;
    private Set<@NotBlank String> fileRejectPatterns = Sets.newHashSet();
    private Set<@NotBlank String> directoryRejectPatterns = Sets.newHashSet();
    private Set<@NotBlank String> directoryTraversalRejectPatterns = Sets.newHashSet();

}
