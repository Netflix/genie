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

package com.netflix.genie.web.util;

import com.netflix.genie.web.services.AgentFilterService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Abstract base class for {@link AgentFilterService.AgentMetadataInspector} based on regex matching.
 * Can be configure to accept or reject in case of match. It is a pass-through otherwise.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
abstract class BaseRegexAgentMetadataInspector implements AgentFilterService.AgentMetadataInspector {

    private final AtomicReference<Pattern> patterncReference = new AtomicReference<>();
    private final AgentFilterService.InspectionReport.InspectionDecision decisionIfMatch;
    private String lastCompiledPattern;

    BaseRegexAgentMetadataInspector(
        final AgentFilterService.InspectionReport.InspectionDecision decisionIfMatch
    ) {

        this.decisionIfMatch = decisionIfMatch;
    }

    AgentFilterService.InspectionReport inspectWithPattern(
        @Nullable final String currentPatternString,
        @Nullable final String metadataAttribute
    ) {
        final Pattern currentPattern = maybeReloadPattern(currentPatternString);

        if (StringUtils.isBlank(metadataAttribute)) {
            return new AgentFilterService.InspectionReport(
                AgentFilterService.InspectionReport.InspectionDecision.CONTINUE,
                "Metadata attribute not set"
            );
        } else if (currentPattern == null) {
            return new AgentFilterService.InspectionReport(
                AgentFilterService.InspectionReport.InspectionDecision.CONTINUE,
                "Pattern not not set"
            );
        } else if (currentPattern.matcher(metadataAttribute).matches()) {
            return new AgentFilterService.InspectionReport(
                decisionIfMatch,
                "Attribute: " + metadataAttribute + " matches: " + currentPattern.pattern()
            );
        } else {
            return new AgentFilterService.InspectionReport(
                AgentFilterService.InspectionReport.InspectionDecision.CONTINUE,
                "Attribute: " + metadataAttribute + " does not match: " + currentPattern.pattern()
            );
        }
    }

    private Pattern maybeReloadPattern(@Nullable final String currentPatternString) {
        synchronized (this) {
            if (StringUtils.isBlank(currentPatternString)) {
                patterncReference.set(null);
                lastCompiledPattern = null;
            } else if (!currentPatternString.equals(lastCompiledPattern)) {
                try {
                    final Pattern newPattern = Pattern.compile(currentPatternString);
                    patterncReference.set(newPattern);
                } catch (final PatternSyntaxException e) {
                    patterncReference.set(null);
                    log.error("Failed to load pattern: {}", currentPatternString, e);
                }
                lastCompiledPattern = currentPatternString;
            }
        }
        return patterncReference.get();
    }
}
