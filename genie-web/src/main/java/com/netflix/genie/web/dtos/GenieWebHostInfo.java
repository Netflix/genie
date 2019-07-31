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
package com.netflix.genie.web.dtos;

import com.netflix.genie.common.internal.util.GenieHostInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * Extension of {@link GenieHostInfo} which adds metadata specific to the web server.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true, callSuper = true)
@ToString(doNotUseGetters = true, callSuper = true)
public class GenieWebHostInfo extends GenieHostInfo {

    @Min(value = 1, message = "The minimum value for the RPC port is 1")
    @Max(value = 65_535, message = "The maximum value for the RPC port is 65,535")
    private final int rpcPort;

    /**
     * Constructor.
     *
     * @param hostname The hostname of this Genie web instance
     * @param rpcPort  The port the RPC server is listening on to receive calls from Agent instances
     */
    public GenieWebHostInfo(final String hostname, final int rpcPort) {
        super(hostname);
        this.rpcPort = rpcPort;
    }
}
