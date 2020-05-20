/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.selectors;

import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import org.springframework.validation.annotation.Validated;

/**
 * Interface for any classes which provide a way to select a {@link Command} from a set of commands
 * which matched criterion provided by a user in a {@link JobRequest}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface CommandSelector extends ResourceSelector<Command, CommandSelectionContext> {
}
