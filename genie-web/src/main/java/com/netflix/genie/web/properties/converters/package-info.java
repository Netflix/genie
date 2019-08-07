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
 * A package which contains implementations of {@link org.springframework.core.convert.converter.Converter} to convert
 * between String representations of properties and more complex objects that can be used more effectively
 * programmatically without spreading such conversion all over the code.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ParametersAreNonnullByDefault
package com.netflix.genie.web.properties.converters;

import javax.annotation.ParametersAreNonnullByDefault;
