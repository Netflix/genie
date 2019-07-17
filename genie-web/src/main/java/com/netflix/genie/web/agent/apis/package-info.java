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

/**
 * Any APIs the server will expose for the Agent to connect to. These are kept separate from the ones in the main
 * APIs package as those are for end users and these are for more Genie system internals.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ParametersAreNonnullByDefault
package com.netflix.genie.web.agent.apis;

import javax.annotation.ParametersAreNonnullByDefault;
