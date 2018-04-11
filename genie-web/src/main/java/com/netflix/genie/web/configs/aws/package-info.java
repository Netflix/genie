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

/**
 * Spring configuration classes for running on AWS. Configurations should all have @Profile("aws)
 * so they are enabled when the AWS profile is activated. Override default beans.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ParametersAreNonnullByDefault
package com.netflix.genie.web.configs.aws;

import javax.annotation.ParametersAreNonnullByDefault;
