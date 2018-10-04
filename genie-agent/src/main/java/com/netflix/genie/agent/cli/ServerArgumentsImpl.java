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
import com.beust.jcommander.validators.PositiveInteger;
import lombok.Getter;

/**
 * Implementation of ServerArguments delegate.
 */
@Getter
class ServerArgumentsImpl implements ArgumentDelegates.ServerArguments {

    @Parameter(
        names = {"--serverHost"},
        description = "Server hostname or address",
        validateWith = ArgumentValidators.StringValidator.class
    )
    private String serverHost = "genie.prod.netflix.net";

    @Parameter(
        names = {"--serverPort"},
        description = "Server port",
        validateWith = ArgumentValidators.PortValidator.class
    )
    private int serverPort = 7979;

    @Parameter(
        names = {"--rpcTimeout"},
        description = "Timeout for blocking RPC calls in seconds",
        validateWith = PositiveInteger.class
    )
    private long rpcTimeout = 30;
}
