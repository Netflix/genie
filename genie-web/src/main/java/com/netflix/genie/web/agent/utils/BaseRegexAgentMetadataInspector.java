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
package com.netflix.genie.web.agent.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Abstract base class for {@link AgentMetadataInspector} based on regex matching.
 * Can be configure to accept or reject in case of match and do the opposite in case of no match.
 * If a valid regex is not provided, this inspector accepts.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
abstract class BaseRegexAgentMetadataInspector implements AgentMetadataInspector {

    private final AtomicReference<Pattern> patternReference = new AtomicReference<>();
    private final InspectionReport.Decision decisionIfMatch;
    private String lastCompiledPattern;

    BaseRegexAgentMetadataInspector(
        final InspectionReport.Decision decisionIfMatch
    ) {
        this.decisionIfMatch = decisionIfMatch;
    }

    InspectionReport inspectWithPattern(
        @Nullable final String currentPatternString,
        @Nullable final String metadataAttribute
    ) {
        final Pattern currentPattern = maybeReloadPattern(currentPatternString);

        if (currentPattern == null) {
            return InspectionReport.newAcceptance("Pattern not not set");
        } else if (StringUtils.isBlank(metadataAttribute)) {
            return InspectionReport.newRejection("Metadata attribute not set");
        } else if (currentPattern.matcher(metadataAttribute).matches()) {
            return new InspectionReport(
                decisionIfMatch,
                "Attribute: " + metadataAttribute + " matches: " + currentPattern.pattern()
            );
        } else {
            return new InspectionReport(
                InspectionReport.Decision.flip(decisionIfMatch),
                "Attribute: " + metadataAttribute + " does not match: " + currentPattern.pattern()
            );
        }
    }

    private Pattern maybeReloadPattern(@Nullable final String currentPatternString) {
        synchronized (this) {
            if (StringUtils.isBlank(currentPatternString)) {
                patternReference.set(null);
                lastCompiledPattern = null;
            } else if (!currentPatternString.equals(lastCompiledPattern)) {
                try {
                    final Pattern newPattern = Pattern.compile(currentPatternString);
                    patternReference.set(newPattern);
                } catch (final PatternSyntaxException e) {
                    patternReference.set(null);
                    log.error("Failed to load pattern: {}", currentPatternString, e);
                }
                lastCompiledPattern = currentPatternString;
            }
        }
        return patternReference.get();
    }
}
