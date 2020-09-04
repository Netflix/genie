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
package com.netflix.genie.agent.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;

/**
 * Properties for {@link com.netflix.genie.agent.execution.services.JobSetupService}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@Setter
@Validated
public class JobSetupServiceProperties {
    /**
     * The regular expression to filter which environment variable are dumped into {@code env.log} before a job is
     * launched. This is provided so that sensitive environment variables can be filtered out.
     * Environment dumping can also be completely disabled by providing an expression that matches nothing (such as
     * {@code '^$'}. The value of this property is injected into a bash script between single quotes.
     */
    @NotEmpty
    private String environmentDumpFilterExpression = ".*";

    /**
     * Switch to invert the matching of {@code environmentDumpFilterExpression}.
     * If {@code false} (default) environment entries matching the expression are dumped, and the rest are filtered out.
     * If {@code true} environment entries NOT matching the expression are dumped, the rest are filtered out.
     */
    private boolean environmentDumpFilterInverted;
}
