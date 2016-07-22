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
package com.netflix.genie.client.interceptors;

import com.google.common.net.HttpHeaders;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

/**
 * An interceptor class that updates the User Agent String of the request with user info.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class UserAgentInsertInterceptor implements Interceptor {

    private static final String DEFAULT_USER_AGENT_STRING = "Genie Java Client";

    private final String userAgent;

    /**
     * Constructor.
     *
     * @param userAgent The user agent string to use
     */
    public UserAgentInsertInterceptor(final String userAgent) {
        if (StringUtils.isEmpty(userAgent)) {
            this.userAgent = DEFAULT_USER_AGENT_STRING;
        } else {
            this.userAgent = userAgent;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Response intercept(final Chain chain) throws IOException {
        final Request newRequest = chain
            .request()
            .newBuilder()
            .addHeader(HttpHeaders.USER_AGENT, this.userAgent)
            .build();

        log.debug("Sending request {} on {} {}", newRequest.url(), chain.connection(), newRequest.headers());

        return chain.proceed(newRequest);
    }
}
