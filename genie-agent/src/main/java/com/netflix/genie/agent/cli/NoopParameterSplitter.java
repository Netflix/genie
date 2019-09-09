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
package com.netflix.genie.agent.cli;

import com.beust.jcommander.converters.IParameterSplitter;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Argument splitter that does not split arguments.
 *
 * @author mprimi
 * @since 4.0.0
 */
class NoopParameterSplitter implements IParameterSplitter {

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> split(final String value) {
        return Lists.newArrayList(value);
    }
}
