/*
 *
 *  Copyright 2018 Netflix, Inc.
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

import com.beust.jcommander.Parameter;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * Implementation of CacheArguments delegate.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@Component
class CacheArgumentsImpl implements ArgumentDelegates.CacheArguments {

    @VisibleForTesting
    static final String DEFAULT_CACHE_PATH = "/tmp/genie/cache";

    @Parameter(
        names = {"--cacheDirectory"},
        description = "Location of the Genie Agent dependencies cache",
        converter = ArgumentConverters.FileConverter.class,
        validateWith = ArgumentValidators.StringValidator.class
    )
    private File cacheDirectory = new File(DEFAULT_CACHE_PATH);
}
